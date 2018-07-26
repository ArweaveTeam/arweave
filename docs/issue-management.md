# Issue Management

## Motivation

Like the **Arweave** code base is hosted on Github we also manage all issues using the according issue services. A defined set of tags helps sorting the issues as well as milestones help to group them for upcoming iterations and releases.

## Process

### Reporting

New entered issues should focus on ideas, problems, and solutions. They should match following criteria:

- Short but expressive title
- Context inside the comment describing what steps or thoughts led to the new issue.
- If possible, e.g. when added internally and according information exists, add references to issues, pull request, members, or external resources.
- Any helpful information leading to a solution is welcome.

### Triage

New issues are triaged by assigning three different labels for *priority*, *status*, and *type*. The concrete labels are prefixed by these three groups:

- **Priority**
  - *Priority: High* - Issues like bugs, that needed to be fixed immediately.
  - *Priority: Normal* - Almost all issues adding new features of improving the coding.
  - *Priority: Low* - Issues representing long-term plannings. Can later be re-prioritised.
- **Type**
  - *Type: Bug* - Defects inside already delivered or in development situated software.
  - *Type: Enhancement* - New features enhancing the functionality of the system.
  - *Type: Maintenance* - Internal changes to improve speed, robustness, or maintainability without adding new features.
  - *Type: Question* - Open topics to be discussed and/or cleared.
- **Status**
  - *Status: Available* - Initially triaged, possibly already assigned, but not yet started.
  - *Status: In Progress* - Started to be solved, the person(s) the issue is assigned to is/are responsible for it.
  - *Status: On Hold* - Temporarily stopped due to low resources, dependencies, unplanned high priority issues, or similar.
  - *Status: Done* - Solution is in review process but not yet accepted and merged, so issue cannot be closed.
  - *Status: Abandoned* - For issues that won't be solved.

### Solution

For the solving of an open issue assign it to you and change the status to *Status: In Progress*. Create an according branch like described in [version control](version-control.md) for your work. Once done and in review change the status to *Status: Done*. Once review is done and solution is merged the issue can be closed.

Sometimes issues won't be solved. This may happen based on analysis, on the project progress in other areas, or in case of duplicates. Here change the status to *Status: Abandoned* and document the reason before closing it.
