# tidaload

A small Rust CLI for downloading TIDAL tracks, albums, and playlists.

## Usage

Log in first:

```sh
tidaload login
```

Download a track, album, or playlist URL:

```sh
tidaload "https://tidal.com/browse/track/3083287"
tidaload "https://tidal.com/browse/album/147569387"
tidaload "https://tidal.com/browse/playlist/{playlist-id}"
```

Raw IDs are supported when the resource kind is provided:

```sh
tidaload --kind track 3083287
tidaload --kind album 147569387
```

Useful options:

```sh
tidaload --concurrency 8 "https://tidal.com/browse/playlist/{playlist-id}"
```

By default, album and playlist downloads use at most 2 concurrent track
downloads. Later tracks start with a small irregular delay to avoid bursty
request patterns. `--concurrency` can override the default for a single run.
The CLI prints coarse global progress and per-track activity, including DASH
segment progress, so long downloads visibly continue moving.

Downloads are always saved under the current Linux user's Music folder.
Before writing, tidaload deletes any existing track file or album/playlist folder
with the same generated name. It does not keep a downloaded-state database.
Each track file embeds the TIDAL album cover as MP4 artwork when cover metadata
is available.

Audio quality is fixed to lossless playback. tidaload first requests TIDAL's
`HI_RES_LOSSLESS` FLAC/DASH manifest because the legacy `LOSSLESS` playback
endpoint can be downgraded by TIDAL to AAC `HIGH`; if FLAC/DASH is unavailable,
it falls back to `LOSSLESS`. DASH audio is saved as an `.m4a` fragmented MP4
container with a FLAC audio stream. DASH media segments are downloaded with
limited per-track concurrency and written back in segment order.

DNS resolution is handled through DNS-over-HTTPS using `dns.google`, with a short
in-process cache. This avoids relying on the local Linux resolver for TIDAL API
and audio CDN hosts.

The default config file is `~/.config/tidaload/config.toml`.
