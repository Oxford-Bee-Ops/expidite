# type: ignore
# /usr/bin/env python3
import os
import random
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import torch
import torchaudio

from sensor_core import configuration as root_cfg

logger = root_cfg.setup_logger("sensor_core")

# Fix the seed for reproducability
np.random.seed(42)

MODE_TRAINING = "Training"
MODE_PRODUCTION = "Production"

############################################################################################################
#
# Audio transformation functions
#
############################################################################################################


# Load an audio file. Return the signal as a tensor and the sample rate
def audio_open(audio_file):
    sig, sr = torchaudio.load(audio_file, backend="soundfile")
    return (sig, sr)


# Convert the given audio to the desired number of channels
def audio_rechannel(aud, new_channel):
    sig, sr = aud

    if sig.shape[0] == new_channel:
        # Nothing to do
        return aud

    if new_channel == 1:
        # Convert from stereo to mono by selecting only the first channel
        resig = sig[:1, :]
    else:
        # Convert from mono to stereo by duplicating the first channel
        resig = torch.cat([sig, sig])

    return (resig, sr)


# Since Resample applies to a single channel, we resample one channel at a time
def audio_resample(aud, newsr):
    sig, sr = aud

    if sr == newsr:
        # Nothing to do
        return aud

    num_channels = sig.shape[0]
    # Resample first channel
    resig = torchaudio.transforms.Resample(sr, newsr)(sig[:1, :])
    if num_channels > 1:
        # Resample the second channel and merge both channels
        retwo = torchaudio.transforms.Resample(sr, newsr)(sig[1:, :])
        resig = torch.cat([resig, retwo])

    return (resig, newsr)


# Pad (or truncate) the signal to a fixed length 'max_ms' in milliseconds
def audio_pad_trunc(aud, max_ms):
    sig, sr = aud
    num_rows, sig_len = sig.shape
    max_len = sr // 1000 * max_ms

    if sig_len > max_len:
        # Truncate the signal to the given length
        sig = sig[:, :max_len]

    elif sig_len < max_len:
        # Length of padding to add at the beginning and end of the signal
        pad_begin_len = random.randint(0, max_len - sig_len)
        pad_end_len = max_len - sig_len - pad_begin_len

        # Pad with 0s
        pad_begin = torch.zeros((num_rows, pad_begin_len))
        pad_end = torch.zeros((num_rows, pad_end_len))

        sig = torch.cat((pad_begin, sig, pad_end), 1)

    return (sig, sr)


# Shifts the signal to the left or right by some percent. Values at the end
# are 'wrapped around' to the start of the transformed signal.
def audio_time_shift(aud, shift_limit):
    sig, sr = aud
    _, sig_len = sig.shape
    shift_amt = int(random.random() * shift_limit * sig_len)
    return (sig.roll(shift_amt), sr)


# Generate a Spectrogram
def audio_spectro_gram(aud, n_mels, n_fft, hop_len):
    sig, sr = aud
    top_db = 80

    # spec has shape [channel, n_mels, time], where channel is mono, stereo etc
    # 2, 64, 32 (for 2 seconds)
    spec = torchaudio.transforms.MelSpectrogram(
        sr, n_fft=n_fft, hop_length=hop_len, n_mels=n_mels, f_max=2400.0
    )(sig)

    # Convert to decibels
    spec = torchaudio.transforms.AmplitudeToDB(top_db=top_db)(spec)
    return spec


# Augment the Spectrogram by masking out some sections of it in both the frequency
# dimension (ie. horizontal bars) and the time dimension (vertical bars) to prevent
# overfitting and to help the model generalise better. The masked sections are
# replaced with the mean value.
def audio_spectro_augment(spec, max_mask_pct, n_freq_masks, n_time_masks):
    _, n_mels, n_steps = spec.shape
    mask_value = spec.mean()
    aug_spec = spec

    freq_mask_param = max_mask_pct * n_mels
    for _ in range(n_freq_masks):
        aug_spec = torchaudio.transforms.FrequencyMasking(freq_mask_param)(aug_spec, mask_value)

    time_mask_param = max_mask_pct * n_steps
    for _ in range(n_time_masks):
        aug_spec = torchaudio.transforms.TimeMasking(time_mask_param)(aug_spec, mask_value)

    return aug_spec


# Static method to prep audio files for ML training or evaluation
def audio_prep_audio(audio_file, sr, channel, duration, mode):
    aud = audio_open(audio_file)
    # Some sounds have a higher sample rate, or fewer channels compared to the
    # majority. So make all sounds have the same number of channels and same
    # sample rate. Unless the sample rate is the same, the pad_trunc will still
    # result in arrays of different lengths, even though the sound duration is
    # the same.
    reaud = audio_resample(aud, sr)
    rechan = audio_rechannel(reaud, channel)
    dur_aud = audio_pad_trunc(rechan, duration)
    if mode == MODE_TRAINING:
        shift_aud = audio_time_shift(dur_aud, 0.1)
    else:
        shift_aud = dur_aud
    sgram = audio_spectro_gram(shift_aud, n_mels=64, n_fft=512, hop_len=None)
    if mode == MODE_TRAINING:
        aug_sgram = audio_spectro_augment(sgram, max_mask_pct=0.05, n_freq_masks=1, n_time_masks=1)
    else:
        aug_sgram = sgram
    return aug_sgram


def audio_scale_minmax(X, min=0.0, max=1.0):
    X_std = (X - X.min()) / (X.max() - X.min())
    X_scaled = X_std * (max - min) + min
    return X_scaled


# Method used by RPI and IOT to convert audio files to images for classification
def save_wav_as_image(audio_file: Path) -> Path:
    aud = audio_open(audio_file)
    reaud = audio_resample(aud, 8000)
    dur_aud = audio_pad_trunc(reaud, 2000)
    sgram = audio_spectro_gram(dur_aud, n_mels=64, n_fft=512, hop_len=None)
    img = audio_scale_minmax(sgram[0])

    # The resulting array is 64x63.  We need to extend it to 64x64, so we add a column of zeroes
    img = np.concatenate((img, np.zeros((64, 1))), axis=1)

    # Flip the image vertically, so that frequency 0 starts at the bottom
    img = np.flip(img, axis=0)

    # Save the image to file as a png
    # If the file already exists, overwrite it
    img_file = audio_file.with_suffix(".png")
    if os.path.exists(img_file):
        os.remove(img_file)
    plt.imsave(img_file, img, cmap="Greys")

    return img_file


# Convert a directory of wav files to images
def wav_to_image(target_dir: Path, sub_list) -> None:
    # Loop the directories in the test_dir/Classifier directory and run save_wav_as_image on each wav file
    for subdir in sub_list:
        wav_files = list(target_dir.joinpath(subdir).glob("*.wav"))
        png_files = list(target_dir.joinpath(subdir).glob("*.png"))
        png_files = [x.with_suffix("wav") for x in png_files]
        wav_files = [x for x in wav_files if x not in png_files]
        logger.info(f"Found {len(wav_files)!s} wav files in {subdir} without png")
        for wav_file in wav_files:
            save_wav_as_image(wav_file)
