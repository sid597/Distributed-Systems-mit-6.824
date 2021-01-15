This is a lab project to implement Key-Value service based on raft for the MIT 6.824 graduate course.

The service supports three operations: `Put(key, value)`, `Append(key, arg)`, and `Get(key)`. It maintains a 
simple database of key/value pairs. Keys and values are strings. `Put()` replaces the value for a particular
key in the database, `Append(key, arg)` appends arg to key's value, and `Get()` fetches the current value for
a key. Each client talks to the service through a `Clerk` with Put/Append/Get methods. A Clerk manages
RPC interactions with the servers.

**Architecture**

Each of the key/value servers(kvservers) will have an associated Raft peer. Clerks send `Put()`, `Append()`,
and `Get()` RPCs to the kvserver whose associated Raft is the leader. The kvserver code submits the Put/Append
/Get operation to Raft, so that the Raft log holds a sequence of Put/Append/Get operations. All of the
kvservers execute operations from the Raft log in order, applying the operations to their key/value 
databases; the intent is for the servers to maintain identical replicas of the key/value database.

A `Clerk` sometimes doesn't know which kvserver is the Raft leader. If the `Clerk` sends an RPC to the wrong 
kvserver, or if it cannot reach the kvserver, the `Clerk` re-tries by sending to a different kvserver.
If the key/value service commits the operation to its Raft log (and hence applies the operation to the
key/value state machine), the leader reports the result to the `Clerk` by responding to its RPC. If the 
operation failed to commit (for example, if the leader was replaced), the server reports an error, and 
the `Clerk` retries with a different server.

**My experience**

This was relatively easier to implement than Raft algorithm. I learned how to use channels 
