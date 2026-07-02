import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite")

# Video files are orders of magnitude larger than any other data we produce, so they are the first thing
# we sacrifice when the spool disk budget is exceeded during a long outage.
_VIDEO_SUFFIXES = {f".{api.FORMAT.MP4.value}", f".{api.FORMAT.AVI.value}", f".{api.FORMAT.H264.value}"}

# Suffix for files that are still being written; renamed away on completion so a crash mid-write never
# leaves a half-file that the drain would upload.
_PART_SUFFIX = ".part"


@dataclass
class SpooledUpload:
    """A block-blob upload persisted to the disk spool, awaiting connectivity."""

    path: Path
    dst_container: str
    storage_tier: api.StorageTier


@dataclass
class SpooledAppend:
    """A fragment of append-blob (CSV) data persisted to the disk spool, awaiting connectivity.

    Each fragment is a complete CSV (headers + rows) named <uuid>.csv inside a directory named after the
    destination blob, so concurrent writers never contend and a fragment can be uploaded via the normal
    append path (which drops the header row if the remote blob already exists).
    """

    path: Path
    dst_container: str
    dst_fname: str


##############################################################################################################
# DiskSpool
#
# Persistent on-disk overflow store for cloud uploads. During a network outage (or under memory pressure)
# the AsyncCloudConnector diverts uploads here instead of retrying them in RAM, because both the upload
# queue and TMP_DIR are memory-backed on SD-card devices. The spool lives on real disk (see
# root_cfg.SPOOL_DIR) so its contents survive reboots; the AsyncCloudConnector drains it back to the cloud
# once connectivity returns, including after a restart.
#
# Layout:
#   <root>/upload/<container>/<TIER>/<filename>      block-blob uploads (tier encoded in the path)
#   <root>/append/<container>/<blob_name>/<uuid>.csv append-blob CSV fragments
##############################################################################################################
class DiskSpool:
    def __init__(self, root: Path | None = None) -> None:
        self._lock = Lock()
        self.videos_binned = 0
        self.root = self._create_root(root)
        self._upload_dir = self.root / "upload"
        self._append_dir = self.root / "append"
        self._upload_dir.mkdir(parents=True, exist_ok=True)
        self._append_dir.mkdir(parents=True, exist_ok=True)
        # Clean up half-written files from a previous run that crashed mid-spool.
        for part in self.root.rglob(f"*{_PART_SUFFIX}"):
            part.unlink(missing_ok=True)
        self._size_bytes = sum(f.stat().st_size for f in self.root.rglob("*") if f.is_file())

    @staticmethod
    def _create_root(root: Path | None) -> Path:
        """Create and return the spool root, falling back to alternatives if it isn't usable.

        SPOOL_DIR is normally created by the installer with the right ownership. If it's missing and we
        can't create it (e.g. the installer hasn't been re-run since this feature shipped), fall back to a
        subdirectory of DIAGS_DIR, which is also on persistent storage and owned by the service user. The
        final fallback inside ROOT_WORKING_DIR is memory-backed on SD-card devices and provides no
        persistence; it only exists so the connector can keep functioning on a misconfigured device.
        """
        candidates = [root] if root is not None else [root_cfg.SPOOL_DIR, root_cfg.DIAGS_DIR / "spool"]
        for candidate in candidates:
            try:
                candidate.mkdir(parents=True, exist_ok=True)
                # mkdir succeeding doesn't prove we can write (the dir may be root-owned); check explicitly.
                probe = candidate / f"probe_{uuid.uuid4().hex}{_PART_SUFFIX}"
                probe.touch()
                probe.unlink()
            except OSError as e:
                # Expected on devices where the installer hasn't created the directory yet; the final
                # fallback below raises the customer-facing fault.
                logger.warning(f"Spool directory {candidate} is not usable: ({e!s})")
            else:
                return candidate
        fallback = root_cfg.ROOT_WORKING_DIR / "spool"
        logger.error(
            f"{root_cfg.RAISE_WARN()}No persistent spool directory available; using {fallback} which will "
            "NOT survive a reboot"
        )
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback

    ##########################################################################################################
    # Writing to the spool
    ##########################################################################################################
    def spool_upload(
        self, dst_container: str, src_file: Path, storage_tier: api.StorageTier, move: bool
    ) -> bool:
        """Persist one block-blob upload to the spool.

        move=True transfers ownership of src_file to the spool (the caller wanted delete_src semantics);
        move=False leaves the caller's file untouched and spools a copy.

        Returns False if the file was binned instead of spooled (video over budget, or disk full).
        """
        try:
            nbytes = src_file.stat().st_size
        except OSError:
            logger.exception(f"{root_cfg.RAISE_WARN()}Cannot spool {src_file}; file not accessible")
            return False

        is_video = src_file.suffix.lower() in _VIDEO_SUFFIXES
        if not self._make_room(nbytes, is_video):
            self._record_binned(src_file)
            if move:
                src_file.unlink(missing_ok=True)
            return False

        dst_dir = self._upload_dir / dst_container / storage_tier.name
        dst_dir.mkdir(parents=True, exist_ok=True)
        try:
            with self._lock:
                dst = dst_dir / src_file.name
                if dst.exists():
                    # File naming is FAIR-unique per record, so an existing entry with the same name is the
                    # same record (e.g. spooled once as a safety copy and again on upload failure); replace.
                    self._size_bytes = max(0, self._size_bytes - dst.stat().st_size)
                    dst.unlink()
                part = dst.with_name(dst.name + _PART_SUFFIX)
                # shutil.move across filesystems (tmpfs -> disk) is a copy+unlink, so stage via a .part name
                # and rename into place; rename within one filesystem is atomic.
                if move:
                    shutil.move(src_file, part)
                else:
                    shutil.copy2(src_file, part)
                part.rename(dst)
                self._size_bytes += nbytes
        except OSError:
            logger.exception(f"{root_cfg.RAISE_WARN()}Failed to spool {src_file} to disk")
            return False
        logger.debug(f"Spooled upload {dst} ({nbytes:,} bytes) for container {dst_container}")
        return True

    def spool_append(self, dst_container: str, dst_fname: str, data: list[str]) -> bool:
        """Persist one block of append data (a complete CSV including headers) to the spool.

        Returns False if the data could not be written (disk full or unwritable).
        """
        nbytes = sum(len(line) for line in data)
        if not self._make_room(nbytes, incoming_is_video=False):
            logger.error(f"{root_cfg.RAISE_WARN()}Spool cannot make room; dropping append to {dst_fname}")
            return False

        fragment_dir = self._append_dir / dst_container / dst_fname
        fragment_dir.mkdir(parents=True, exist_ok=True)
        fragment = fragment_dir / f"{uuid.uuid4().hex}.csv"
        part = fragment.with_name(fragment.name + _PART_SUFFIX)
        try:
            with part.open("w", newline="") as f:
                f.writelines(data)
            actual_bytes = part.stat().st_size
            part.rename(fragment)
        except OSError:
            logger.exception(f"{root_cfg.RAISE_WARN()}Failed to spool append data for {dst_fname}")
            part.unlink(missing_ok=True)
            return False
        with self._lock:
            self._size_bytes += actual_bytes
        logger.debug(f"Spooled append fragment {fragment} for {dst_container}/{dst_fname}")
        return True

    ##########################################################################################################
    # Reading & draining the spool
    ##########################################################################################################
    @staticmethod
    def _safe_stat(f: Path, attr: str) -> float:
        """st_mtime/st_size that tolerates the file being evicted by another thread mid-listing."""
        try:
            return float(getattr(f.stat(), attr))
        except OSError:
            return 0.0

    def pending_appends(self) -> list[SpooledAppend]:
        """All spooled append fragments, oldest first (per-blob fragments stay in write order)."""
        items = [
            SpooledAppend(path=f, dst_container=f.parent.parent.name, dst_fname=f.parent.name)
            for f in self._append_dir.glob("*/*/*.csv")
        ]
        items.sort(key=lambda i: self._safe_stat(i.path, "st_mtime"))
        return items

    def pending_uploads(self) -> list[SpooledUpload]:
        """All spooled uploads, oldest first."""
        items = []
        for f in self._upload_dir.glob("*/*/*"):
            if not f.is_file() or f.suffix == _PART_SUFFIX:
                continue
            items.append(
                SpooledUpload(path=f, dst_container=f.parent.parent.name, storage_tier=self._tier_of(f))
            )
        items.sort(key=lambda i: self._safe_stat(i.path, "st_mtime"))
        return items

    def smallest_pending(self) -> SpooledAppend | SpooledUpload | None:
        """The smallest spooled item - used as a cheap connectivity probe while offline."""
        items: list[SpooledAppend | SpooledUpload] = []
        items.extend(self.pending_appends())
        items.extend(self.pending_uploads())
        if not items:
            return None
        return min(items, key=lambda i: self._safe_stat(i.path, "st_size"))

    def remove(self, item: SpooledAppend | SpooledUpload) -> None:
        """Remove a successfully-drained item from the spool."""
        try:
            nbytes = item.path.stat().st_size
            item.path.unlink()
        except OSError:
            return
        with self._lock:
            self._size_bytes = max(0, self._size_bytes - nbytes)

    def has_data(self) -> bool:
        return any(self.pending_appends()) or any(self.pending_uploads())

    @property
    def size_bytes(self) -> int:
        return self._size_bytes

    @staticmethod
    def _tier_of(f: Path) -> api.StorageTier:
        try:
            return api.StorageTier[f.parent.name]
        except KeyError:
            return api.StorageTier.HOT

    ##########################################################################################################
    # Disk budget
    ##########################################################################################################
    def _make_room(self, nbytes: int, incoming_is_video: bool) -> bool:
        """Ensure the spool can accept nbytes more data, evicting the oldest spooled videos if needed.

        Returns False if the incoming data should be binned instead: it is a video and no room can be made,
        or the disk is physically too full to accept it.
        """
        with self._lock:
            while self._over_budget(nbytes):
                victim = self._oldest_video()
                if victim is None:
                    break
                try:
                    victim_bytes = victim.stat().st_size
                    victim.unlink()
                except OSError:
                    break
                self._size_bytes = max(0, self._size_bytes - victim_bytes)
                self._record_binned(victim)

            if not self._over_budget(nbytes):
                return True
            if incoming_is_video:
                return False
            # Non-video data (CSVs, logs) is small and precious; allow it to overshoot SPOOL_MAX_BYTES as
            # long as the disk itself can still take it.
            return self._disk_free() - nbytes > root_cfg.SPOOL_MIN_DISK_FREE_BYTES

    def _over_budget(self, nbytes: int) -> bool:
        return (
            self._size_bytes + nbytes > root_cfg.SPOOL_MAX_BYTES
            or self._disk_free() - nbytes < root_cfg.SPOOL_MIN_DISK_FREE_BYTES
        )

    def _disk_free(self) -> int:
        return shutil.disk_usage(self.root).free

    def _oldest_video(self) -> Path | None:
        videos = [
            f for f in self._upload_dir.glob("*/*/*") if f.is_file() and f.suffix.lower() in _VIDEO_SUFFIXES
        ]
        if not videos:
            return None
        return min(videos, key=lambda f: f.stat().st_mtime)

    def _record_binned(self, f: Path) -> None:
        self.videos_binned += 1
        # The first bin of a run is escalated so fleet operators can see data is being sacrificed; after
        # that, log periodically rather than once per file (a week-long outage can bin thousands of videos).
        if self.videos_binned == 1 or self.videos_binned % 100 == 0:
            logger.error(
                f"{root_cfg.RAISE_WARN()}Spool over budget: binned video {f.name} "
                f"({self.videos_binned} binned so far this run)"
            )
        else:
            logger.info(f"Spool over budget: binned video {f.name}")
