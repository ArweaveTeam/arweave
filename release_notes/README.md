# Arweave Releases

Process for doing an Arweave release

## Run tests

1. Make sure the automated unit tests are green for both [Ubuntu](https://github.com/ArweaveTeam/arweave/actions/workflows/test-amd64-ubuntu-22.04.yml) and [MacOS](https://github.com/ArweaveTeam/arweave/actions/workflows/test-arm64-macos-15.yml)
2. Optionally run the `e2e` test locally (these test can take a couple hours to complete): `./bin/e2e`

## Release procedure

This section explains how arweave is released using Github Actions.

1. find a release version using `N.X.Y.Z.*` format, for example
   `N.9.8.7-alpha2`. During the next step, it will be called
   `${release_version}`.
   
2. create a new release notes containing the instruction of the new
   release.

```sh
mkdir release_notes/${release_version}
touch release_notes/${release_version}/README.md
cat > release_notes/${release_version}/README.md <<EOF
# New release!

Here the message...
EOF
```

3. create a new commit containing the release message updating the version names/numbers in:
   - `rebar.config`
   - `arweave.app.src`
   
4. push this commit to master or via a PR.

```sh
git push
```

5. create a new tag and push it to the repository.

```sh
git tag ${release_version}
git push origin refs/tags/${release_version}
```

6. If the tag match the required specification, a github action
   workflow will be executed to generate the artifacts for the
   release.

7. After the release is complete, create a new commit bumping the `RELEASE_NUMBER` again to differentiate future `master` builds from the latest release. [Example commit.](https://github.com/ArweaveTeam/arweave/commit/882b9e058f18e7eec9fbc5ee8c9b24f089f94c12)
