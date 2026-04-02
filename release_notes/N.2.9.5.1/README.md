# Arweave 2.9.5.1 Patch Release Notes

### This release introduces various stability and validation enhancements.

Several input validation steps could crash on invalid values, in some cases halting the arweave node. 

The patch includes graceful validation of certain inputs and defensive deserialization of local binaries.

We recommend you to install this update as soon as you're able, to address potential node stability issues. 

*NOTE:* No funds are at risk.

## Community involvement

A huge thank you to the following researchers who identified and helped to patch issues addressed in this release!

- Cantina's AppSec agent, Apex (https://www.cantina.security/)
- bbl4de (https://github.com/bbl4de)
- 0xJ3an (https://github.com/0xJ3an)
- windhustler from Burra Security
