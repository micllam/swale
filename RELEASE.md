# Releasing

The `Release` workflow publishes the version of `swale` unless it has already
been published to crates.io. It then creates the tag `swale-v<version>` and a
GitHub release with the notes of `swale/CHANGELOG.md`. The trusted publisher of
the crate on crates.io must specify this repository and the workflow file
`release.yml`.

## The lockfile

Before the release commit, run `cargo update` and the commands of the `lint` and
`test` jobs in `.github/workflows/ci.yml`. Commit `Cargo.lock` on its own, with
the subject `root: update lockfile`.

## The release commit

1. Choose the version from the unreleased entries.
2. In `swale/CHANGELOG.md`, add the heading `## [X.Y.Z] - YYYY-MM-DD` directly
   below `## [Unreleased]`. The workflow takes the first section after
   `Unreleased` as the release notes. Leave the `<!-- vale off -->` marker in
   place.
3. Set the version in `swale/Cargo.toml`, and run `cargo check` to update the
   version of `swale` in `Cargo.lock`.
4. Commit with the subject `release: swale X.Y.Z`.

## Publishing

Once the release commit is on `master` and CI passes, start the `Release`
workflow on `master` from the Actions tab.
