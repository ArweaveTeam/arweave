# Version Control

## Motivation

The **Arweave** client codebase is hosted on Github, the below standards define the process and the criteria for committed code. We aim to adhere to these standards as to make it as easy possible for new contributors to get involved.

## Process

### Workflow

The **Arweave** version control workflow follows the idea of the [Forking Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/forking-workflow). The branches inside the own fork are named by their type, the issue number, and a descriptive name. Prefixes are

- `bug` for bugfixes
- `enhancement` for new features
- `maintenance` for improvements without new features (e.g. for speed, robustness, or code refactoring)
- `question` for answers to questions

#### Examples

- `enhancement/1234/transaction-rate-api` implements issue 1234, a new transaction rate API feature
- `maintenance/2345/improve-block-verification` implements issue 2345, an improvement of the block verification
- `bug/3456/tx-rate-api-memleak` implements issue 3456, it fixes a memory leak added with the new API of issue 1234 above

### Pull Request

Before doing a pull request the latest develop branch of upstream has to be merged and all tests have to run. The pull request itself has to be done against the upstream develop branch. The title has to be descriptive, the description has to list all the changes and possible other helpful information to support the work of the reviewer. The last statement of the comment references the issue so that GitHub can generate a link to it.

#### Example

```
Fix transaction rate API memory leak

- Add timer to rate collector process
- Periodically aggregate transaction rates
- Cleanup collected transaction times

Issue #3456
```

### Review

The reviewer assigns herself takes the responsibility for the code. She can comment the code and push it back to the developer to do the according changes. Once the code looks good for the reviewer she squashes and merges it.

(*to be discussed:* two reviewers; local tests)

## Committing

### All committed code must be commented

All committed code should be fully commented and should aim to fit the styling as detailed in this document. Committing uncommented code is unhelpful to all those maintaining or exploring the project.

### Code pushed to master must work

All code committed to the master branch of the Arweave project should be fully functioning.

This is a **strict** requirement as this is the prime location of where end users will be obtaining the software to join and participate in the network.

### Commits should aim to be as atomic as possible

Code commits should aim to be a single logical change or addition to the codebase, though if not possible all logical alterations should be explained in the commit message, each separated by a comma.

```
- Added generic protocol implementation.
- Removed ar_deprecated.
- Added block shadows, refactored HTTP iface.
```

### Commit message syntax

To keep the repository clean a set structure for commit messages has been decided.

- The message should be preceeded with the dash character followed by a space.
- The first character should be capitalized.
- The message should be terminated with a full stop.
- The message should be succinct.
- Actions taken should be written in past tense.
- Multiple actions should be comma separated.

```
- Added arweave style guide, removed inconsistent styling.
```

(*to be discussed:* https://chris.beams.io/posts/git-commit/)
