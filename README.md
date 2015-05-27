SAS App Engine MCP
========================

Overview
--------

MCP is a daemon that runs on the head node of a SAS App Engine. It connects to
the *rMake Message Bus* and listens for jobs initiated by *mint*. When a job is
received, it is distributed to a *jobmaster* that has a free slot, again via
the message bus. If the jobmaster stops responding, mcp will mark all jobs that
were assigned to it as failed. MCP can also stop queued or running jobs.

Also included is a client library for transmitting jobs to the MCP and polling
their status.
