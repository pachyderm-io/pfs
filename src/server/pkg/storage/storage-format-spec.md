This is an informal description of the storage format spec we will be defining
to help guide the development of Pachyderm's new storage layer. This is a work
in progress so the details are currently subject to change.

**Storage Format Basics:**

- The primitive for this storage format is the tar stream with UStar format headers.
- There are two basic types of tar entries: index and content.
  - Index entries index into other tar streams.
  - Content entries are standard tar entries of type file or directory.
- All tar streams are sorted with respect to the lexicographical order of the
  header names. Duplicate header names are allowed and are interpreted as
  a merge (described in the merge section).
- This storage format:
  - requires an implementation of a content-addressed chunk storage layer.
  - uses protocol buffers as the serialization format for indexes.

**Chunk Storage Layer:**

- The chunk storage layer should:
  - be able to convert arbitrary byte streams into content-addressed chunks and
    store them.
  - provide an interface for creating and accessing ranges within/across these
    byte streams.
- A reference to data within a chunk is stored as a DataRef protocol buffer
  message which includes the chunk's content-address(hash), offset, and size.
- Referencing a range across multiple chunks requires stringing together
  multiple of these single chunk references.
- The hash of the data may also be included if it does not match the chunk hash
  (the data is only part of the chunk).

```
message DataRef {
	Chunk chunk
	string hash
	int64 offset_bytes
	int64 size_bytes
}

message Chunk {
	string hash
}
```

**Index Entry:**

- An index entry contains an index into other tar streams.
- The header name of an index entry is the same as the first header in the
  indexed tar stream.
- Index entries may index a tar stream with only one tar entry or a range of
  tar entries.
    - Index entries that index one tar entry have headers with type flag set to
      'i' and have additional metadata about the single tar entry.
    - Index entries that index a range of tar entries have headers with type
      flag set to 'r' and have additional metadata about the range of tar
      entries.
- The index is stored within an Index protocol buffer message that is
  serialized into the content section of the index entry.

```
message Index {
	Range Range
	DataOp dataOp
}

message Range {
	string lastPath
	// other stuff (bloom filter, stats, etc.)
}

message DataOp {
	repeated DataRef dataRef
	Op op
	repeated Tag tags
}

enum Op {
	APPEND
	OVERWRITE
	DELETE
}

message Tag {
	int64 id
	int64 size_bytes
}
```

- The indexed tar stream is represented by a data operation which may contain
  a sequence of data references that are associated with the operation (there
  should be no data references for a delete). The sequence of data references
  should be stored in the order they should be read to get the final state of
  the underlying tar stream.
- A sequence of tags can be associated with a data operation to communicate the
  relative ordering of data between data operations during merge. The tags are
  ordered in correspondence with the data, such that reading the data
  references and tags together provides enough information to determine the
  mapping from tag to data.

**Merging:**

- A merge is needed when content for a path(s) is spread across multiple tar
  streams.
- The merge operation in general should apply the data operations of the
  path(s) that show up in multiple tar streams, and create a new data operation
  that represents the merged state of the path.
- A merge is represented in the storage format by a sequence of tar entries
  that have overlapping ranges.
  - The range for an index entry that indexes one tar entry and a content entry
    is simply the tar header name.
  - The range for an index entry of that indexes a range of tar entries is the
    range formed by the tar header name and the lastPath field (inclusive).
- Based on the additional metadata associated with certain index entries, it
  may make sense to merge certain files at read time without re-writing storage
  or to merge the data eagerly and re-write storage.

## Garbage Collection

A background garbage collection process will replace the existing `pachctl
garbage-collect` command, which currently requires all pipelines to stop before
garbage collection can begin.

We store two types of objects in object storage, content addressed objects and
semantically addressed objects. Content addressed objects are named based on
their content, semantically named objects have their names passed in and are
identifiers that users of the storage layer defines. In practice these will be
commit names although we may start using it for other identifiers as well. The
important differences between these two types of objects as it pertains to GC is:

- CA objects are referenced by SA objects, and by each other. They cannot be
  deleted by the user, they must be deleted by GC.
- SA objects aren't referenced by other objects, they are deleted by the user
  and this deletion orphans CA objects which allows them to be GCed.

### Requirements

* Always-running background process/service
* No global locks or stoppages required during steady-state operations
* Iterative and rate-limitable for any large operations
* Batches updates from Pachyderm and batches deletes to object storage
* Recovers smoothly from disorderly shutdown
* Capable of becoming shardable with minimal changes

### API

The garbage collector will have two components, a client-side library and a
service that runs in pachd.  The library will act as the single-point of contact
for a client, while coordinating between the pachd service and the database as
necessary.

```go
type Reference struct {
  chunk Chunk
  sourcetype string
  source string
}

func (gcc *GcClient) ReserveChunks(jobID string, chunks []Chunk) {...}
func (gcc *GcClient) UpdateReferences(add []Reference, remove []Reference, releaseJobID string) {...}
```

The garbage collection service will provide a GRPC API for use by the client
library (which should not be used directly).

```proto
message FlushDeletesRequest {
  repeated Chunk chunks;
}

rpc FlushDeletes(FlushDeletesRequest)

message DeleteChunksRequest {
  repeated Chunk chunks;
}

rpc DeleteChunks(DeleteChunksRequest)
```

### Implementation

**Client Library**

`ReserveChunks` will upsert records for the given chunks in the database, as
well as adding temporary references to each of the given chunks using the
specified job-id. If the chunks are currently being deleted, the client library
will contact the pachd service and block until the deletion is confirmed and the
chunk can be safely rewritten, using the `FlushDeletes` RPC.  The temporary
reference can be released via the `releaseJob` parameter to `UpdateReferences`.

`UpdateReferences` will update the reference count of the given chunks in the
database.  If the references drop to zero, the client library will call the 
`DeleteChunks` rpc on the pachd service.

**Pachd Service**

`FlushDeletes` is a synchronization RPC that will block until pending deletes
for the specified chunks have completed.

`DeleteChunks` is an RPC for clients to notify the server that a chunk may no
longer be referenced.  The service will mark the chunk as 'deleting' by setting
a timestamp in the `deleting` field, remove the chunk from object storage, then
remove the row from the database as well as cross-chunk references originating
from that chunk.  `DeleteChunks` may result in recursive calls to `DeleteChunks`
when removing cross-chunk references.  This operation will be aborted on a chunk-
by-chunk basis if a chunk is found to have live references during the atomic
operation to mark it as 'deleting'.  While a chunk is being evaluated for
`DeleteChunks`, it has an entry in a map for synchroniation purposes with
`FlushDeletes` that can be used to wait for the deletion operation to finish
(either to be aborted or succeed), after which it is safe to call `ReserveChunks`
for the given chunk.

#### Persistence

The garbage collector will save changes to a database which will act as the
point of coordination between nodes.

Chunks table spec:
* hash: string (chunk name, primary key)
* deleting: timestamp (one-way)

References table spec:
* chunk: string (chunk name)
* sourcetype: string (the type of the source of the reference 'chunk', 'semantic', or 'job')
* source: string (source of the reference, interpretation is dependent on `sourcetype` field)
* primary key is the compound of (chunk, sourcetype, source)

The `source` field indicates the source of the reference, which can be one of
the following:
* a cross-chunk reference - `sourcetype`: "chunk"
  * a reference from one chunk to another
  * this field should be the hash id of the source chunk of the reference
* a semantic reference - `sourcetype`: "semantic"
  * typically the top-level reference to a chunk
  * this field is the identifier that the chunk is referred to as
* a job reference - `sourcetype`: "job"
  * this is a temporary reference tied to the lifecycle of the job
  * this field is the job id, which can be resolved to check if the job is
 complete and the reference can be removed

Logical states:
Nascent ↔ Live → Deleting → Deleted

* Nascent
  * the chunk has been reserved for writing
  * the chunk may or may not exist in object storage
  * the chunk has one or more temporary semantic references from one or more clients
  * `deleting` should be `nil`
* Live
  * the chunk is referenced either semantically or from clients or other chunks
  * the chunk must exist in object storage
  * `deleting` should be `nil`
* Deleting
  * the chunk's references to other chunks have been removed and it is in the process of being deleted
  * the chunk may or may not exist in object storage
  * `deleting` should be non-`nil`
* Deleted
  * the chunk is no longer present in the database
  * the chunk must not exist in object storage

#### Recovering

When starting up after a disorderly shutdown, the garbage collector will need to
recover missing state.  This will be chunks with stale semantic references from
failed clients, or chunks that were in the process of deleting.  At startup, the
garbage collector service should query these from the database and resume
deletion.

#### Shardability

Garbage collector operations should be shardable by the keyspace of the chunk
hashes.  Multiple garbage collectors could theoretically run across the cluster,
each responsible for a prefix in the keyspace.  The most complicated part would
likely be bootstrapping parallel garbage collectors and routing requests
correctly.  Such a system would allow us to grow the garbage collector past
what one node can handle (i.e. the deletion traffic puts too much load on a
node).
