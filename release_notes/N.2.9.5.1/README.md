# Arweave 2.9.5.1 Patch Release Notes

### This release introduces various stability and validation enhancements.

Several input validation steps could crash on invalid values, in some cases halting the arweave node. 

The patch includes graceful validation of certain inputs and defensive deserialization of local binaries.
