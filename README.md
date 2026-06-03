# tidaload

A small Rust CLI for downloading TIDAL tracks, albums, and playlists.

Linux only now.

Requires `ffmpeg` and `ffprobe` on `PATH` to produce native `.flac` files from all stream formats.

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

Downloads are saved under the current Linux user's Music folder by default.
Use `--download-dir` for a single run:

```sh
tidaload --download-dir ~/Music/TIDAL "https://tidal.com/album/496439179/u"
```

The default config file is `~/.config/tidaload/config.toml`.

Example download directory config:

```toml
[downloads]
download_dir = "/home/alice/Music/TIDAL"
```
