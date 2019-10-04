#Solr + Blobstore
##Overview
This repo introduces a new framework which allows SolrCloud to integrate with an external (typically cloud-based) blobstore. Instead of maintaining a copy of the index on each Solr host, replicating updates to peers, and using a transaction log to maintain consistent ordered updates, Solr hosts will push and pull cores to/from this external store.

For now, SolrCloud can be configured to use blobstore at a collection level. Collections backed by blobstore use a new SHARED replica type. When a Solr node makes an update request to a shared shard, it indexes locally and then pushes the change through to a shared blobstore. Zookeeper manages index versioning and provides a source of truth in the case of concurrent writes. Solr nodes in a cluster will no longer use peer-to-peer replication, and instead will pull updates directly from the shared blobstore.

Please note that this project is a work in progress, and is by no means production-ready. This code is being published early get feedback, which we will incorporate in future work.

In order to modularize these changes and maintain existing functionality, most of the blobstore-related code is isolated to the solr/core/src/java/org/apache/solr/store/blob directory. However, there some key integration touchpoints in HttpSolrCall#init, DistributedZkUpdateProcessor, and CoreContainer#load. These classes all have special handling for blobstore-based shards.

##Pulling from Blobstore
Core pulls are, for the most part, asynchronous. When a replica is queried, it enqueues a pull from blobstore but doesn’t wait for the pull to complete before it executes the query, unless the replica is missing a copy of that core altogether. If your operation requires that local cores are in-sync with blobstore, use the method BlobStoreUtils#syncLocalCoreWithSharedStore.

A more in-depth walkthrough of the pull code:
- BlobCoreSyncer: manages threads that sync between local and blob store, so that if a pull is in progress, we do not create duplicate work.
- Calls into CorePullTracker: creates PullCoreInfo object containing data about the core to be pulled and adds to a deduplicated list.
- This queue of pull objects is polled by the CorePullerFeeder, which uses threads from its dedicated thread pool to execute CorePullTasks.
- CorePullTask: checks if a pull is already underway for this core; if not, executes a pull from blob store. Resolves differences between blob’s version of the core and local version, and stores the updated core

##Pushing to Blobstore
This happens synchronously. On every local commit, we push to blobstore and only ack that the update was successful when it is committed both locally and in the shared store.

A more in-depth walkthrough of the push code:
- DistributedZkUpdateProcessor: once a commit is complete for a SHARED replica (onFinish), we writeToShareStore.
- This calls into CoreUpdateTracker, which creates a PushPullData object containing data about the collection, core, and most recently pulled version of the core on this replica.
- CorePusher: resolves the differences between blob’s version of the core and local version, and pushes the updated version to blob store

##Resolving Local and Blobstore
The SharedStoreResolutionUtil handles resolving diffs between the Solr node’s local copy of a core and the copy in blobstore. It does so by pulling the metadata for the core from blobstore (BlobCoreMetadata), comparing against the local metadata (ServerSideMetadata), and creating a list of segments to push or pull.

##Version Management
Only the leader node can push updates to blobstore. Because a new leader can be elected at any time, there is still a possibility for race conditions on writes to blobstore. In order to maintain a consistent global view of the latest version of a core, we keep version data in Zookeeper.

Zookeeper stores this version data as a random string called metadataSuffix. When a SolrCloud node makes an update request, it first pushes the files to blobstore and then makes a conditional update to the metadataSuffix variable. If Zookeeper rejects the conditional update, the update request fails, and the failure is propagated back to the client.

This communication with Zookeeper is coordinated in the SharedShardMetadataController. The SharedShardMetadataController belongs to the Overseer (i.e. the leader replica).

##Try it yourself
If you want to try this out locally, you can start up SolrCloud with the given blobstore code. The code will default to using the local blobstore client, with /tmp/BlobStoreLocal as the blobstore directory (see LocalStorageClient). You can create a shared collection through the Solr admin UI by setting “shared store based” to true.

Note: if you want to try testing with the S3StorageClient, you need to store a valid S3 bucket name and credentials as environment variables (see S3StorageClient#AmazonS3Configs).


