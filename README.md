# tidaload

A small Rust CLI for downloading TIDAL tracks, albums, and playlists.

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

The default config file is `~/.config/tidaload/config.toml`.
