import sys
from pathlib import Path

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector import CloudConnector


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python upload.py <filename>")
        sys.exit(1)

    filename_str = sys.argv[1]
    try:
        filename = Path(filename_str)
        cc = CloudConnector.get_instance(root_cfg.CLOUD_TYPE)
        cc.upload_to_container("tmp-upload", [filename], delete_src=False)
        print(f"Successfully uploaded {filename} to blobstore.")
    except Exception as e:
        print(f"Failed to upload {filename}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
