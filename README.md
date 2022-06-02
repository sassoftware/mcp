# SAS App Engine MCP -- Archived Repository

**Notice: This repository is part of a Conary/rpath project at SAS that is no longer supported or maintained. Hence, the repository is being archived and will live in a read-only state moving forward. Issues, pull requests, and changes will no longer be accepted.**
 
Overview
--------

MCP is a daemon that runs on the head node of a SAS App Engine. It connects to
the *rMake Message Bus* and listens for jobs initiated by *mint*. When a job is
received, it is distributed to a *jobmaster* that has a free slot, again via
the message bus. If the jobmaster stops responding, mcp will mark all jobs that
were assigned to it as failed. MCP can also stop queued or running jobs.

Also included is a client library for transmitting jobs to the MCP and polling
their status.
