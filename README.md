# tidaload

A small Rust CLI for downloading TIDAL tracks, albums, and playlists.

Linux only for now.

Use it only for TIDAL content you are authorized to access with your account.

## Requirements

- Linux
- Rust 1.85+ to build from source
- `ffmpeg` and `ffprobe` on `PATH`
- A TIDAL account with streaming access

`ffmpeg` and `ffprobe` are required because tidaload produces native `.flac` files from direct and DASH stream formats.

## Install

Build from source:

```sh
cargo install --path .
```

Or build a local release binary:

```sh
cargo build --release --locked
./target/release/tidaload --help
```

## Usage

Log in first:

```sh
tidaload login
```

Download a track, album, or playlist URL:

```sh
tidaload "https://tidal.com/track/526687566/u"
tidaload "https://tidal.com/album/496439179/u"
tidaload "https://tidal.com/playlist/36ea71a8-445e-41a4-82ab-6628c581535d"
```

Raw IDs are supported when `--kind` is supplied:

```sh
tidaload --kind track 526687566
tidaload --kind album 496439179
tidaload --kind playlist 36ea71a8-445e-41a4-82ab-6628c581535d
```

Downloads are saved under the current Linux user's Music folder by default.
Use `--download-dir` for a single run:

```sh
tidaload --download-dir ~/Music/TIDAL "https://tidal.com/album/496439179/u"
```

The default config file is `~/.config/tidaload/config.toml`.
The file is created with owner-only permissions (`0600`) because it stores TIDAL tokens.

Example download directory config:

```toml
[downloads]
download_dir = "/home/alice/Music/TIDAL"
dash_segment_concurrency = 8
```

`--dash-segment-concurrency` overrides DASH segment concurrency for one run. Track download concurrency is fixed at two to reduce TIDAL rate-limit pressure.

## Download Behavior

- Tracks are saved as native `.flac` files when possible.
- `HI_RES_LOSSLESS` is tried first; tidaload falls back to `LOSSLESS` when the high-resolution manifest is missing, MQA, or an unsupported DASH codec.
- Album folders are named `Artist - Album (Year)`.
- Multi-disc albums are split into `Disc N` subdirectories.
- Cover art and audio metadata are embedded when available.
- Existing target album or playlist folders are removed before downloading that collection. Existing target track files are also replaced.
- If TIDAL returns HTTP 429, tidaload records a 10-minute cooldown in the config and exits.

## Development

Run the local checks:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
```

Release tags are handled by `scripts/release.sh`; GitHub Actions builds Linux amd64 and arm64 release assets for `v*` tags.
