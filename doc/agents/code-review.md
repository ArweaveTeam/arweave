# Code review

## Always propose a fix

When you highlight an issue, suggest one or two concrete ways to patch it. A
finding without a proposed fix pushes all the work back onto the author.

## Recommend regression coverage

When a finding warrants regression coverage, recommend the narrowest test level
that can reproduce it reliably: a unit or EUnit test for isolated behavior, or a
Common Test or integration test when the behavior crosses processes or nodes.
Keep each proposed test focused on one issue.

Offer to implement the tests when follow-up changes are in scope. Do not append
a boilerplate question about tests to every review.
