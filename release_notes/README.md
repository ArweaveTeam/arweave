To release a new arweave version using this new
github workflow:

 0. Determine the release version - it must start with
    `N.`. We'll refer to that release version as
    `${release}` in the following instructions.

 1. create a new commit containing a release message
    in `release_notes/${release}/README.md`

    ```
    mkdir release_notes/${release}
    echo "new release" > release_notes/${release}/README.md
    git commit -am "bump version"
    ```

 2. push the new release without any tag for now

    `git push`

 3. create a tag called `${release}`

    `git tag ${release}`

 4. push the tag

    `git push --tags`

 5. a new release should have been created containing
    the message created in step (1).