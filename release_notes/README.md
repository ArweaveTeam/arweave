# Arweave Releases

This document explains how arweave is released using Github Actions.

## Release procedure

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

3. create a new commit containing the release message and the last
   modification required, like modifying the release name in other
   place, or bumping a version number
   
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
