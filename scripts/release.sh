#!/usr/bin/env sh
set -eu

usage() {
  cat <<'EOF'
Usage:
  scripts/release.sh VERSION [--push] [--skip-checks]

Examples:
  scripts/release.sh 0.1.2
  scripts/release.sh v0.1.2 --push

What it does:
  1. Verifies the git worktree is clean and synced with the upstream branch.
  2. Updates Cargo.toml package.version.
  3. Refreshes Cargo.lock.
  4. Runs cargo build/test/clippy with --locked, unless --skip-checks is used.
  5. Commits Cargo.toml and Cargo.lock.
  6. Creates an annotated vVERSION tag.
  7. With --push, pushes the branch and tag to origin.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

run() {
  echo "+ $*"
  "$@"
}

version=""
push_release=0
skip_checks=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --push)
      push_release=1
      ;;
    --skip-checks)
      skip_checks=1
      ;;
    -*)
      die "unknown option: $1"
      ;;
    *)
      [ -z "$version" ] || die "multiple versions provided"
      version="$1"
      ;;
  esac
  shift
done

[ -n "$version" ] || {
  usage
  exit 1
}

version=${version#v}
case "$version" in
  *[!0-9.]*|"")
    die "version must look like 0.1.2 or v0.1.2"
    ;;
esac

tag="v$version"
repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

[ -f Cargo.toml ] || die "Cargo.toml not found"
[ -f Cargo.lock ] || die "Cargo.lock not found"

current_branch=$(git branch --show-current)
[ -n "$current_branch" ] || die "not on a branch"

if [ -n "$(git status --porcelain)" ]; then
  die "worktree is not clean; commit or stash local changes first"
fi

run git fetch origin --tags

upstream=$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null || true)
if [ -z "$upstream" ]; then
  die "current branch has no upstream tracking branch"
fi

set -- $(git rev-list --left-right --count "HEAD...$upstream")
ahead=$1
behind=$2
[ "$ahead" = "0" ] || die "branch is ahead of $upstream; push or rebase before releasing"
[ "$behind" = "0" ] || die "branch is behind $upstream; pull or rebase before releasing"

if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
  die "local tag $tag already exists"
fi
if git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null 2>&1; then
  die "remote tag $tag already exists"
fi

current_version=$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -n 1)
[ -n "$current_version" ] || die "could not read package version from Cargo.toml"
[ "$current_version" != "$version" ] || die "Cargo.toml is already at version $version"

tmp_file=$(mktemp)
sed "0,/^version = \".*\"/s//version = \"$version\"/" Cargo.toml > "$tmp_file"
mv "$tmp_file" Cargo.toml

run cargo check

if [ "$skip_checks" -eq 0 ]; then
  run cargo build --locked --verbose
  run cargo test --locked --verbose
  run cargo clippy --all-targets --all-features --locked -- -D warnings
else
  echo "Skipping cargo build/test/clippy checks."
fi

run git diff --check
run git add Cargo.toml Cargo.lock
run git commit -m "Release $tag"
run git tag -a "$tag" -m "Release $tag"

if [ "$push_release" -eq 1 ]; then
  run git push origin "$current_branch"
  run git push origin "$tag"
  echo "Pushed $tag. GitHub Actions will build release assets for amd64 and arm64."
else
  cat <<EOF

Created release commit and tag locally:
  $tag

To publish and trigger GitHub Actions release assets:
  git push origin $current_branch
  git push origin $tag
EOF
fi
