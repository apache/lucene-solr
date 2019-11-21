package org.apache.solr.store.blob.util;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.CorePushPull;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.blob.metadata.ServerSideMetadata;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil.SharedMetadataResolutionResult;
import org.apache.solr.store.blob.process.BlobDeleteManager;
import org.apache.solr.store.shared.SharedCoreConcurrencyController;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedCoreStage;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController.SharedShardVersionMetadata;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for BlobStore components
 */
public class BlobStoreUtils {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  
  /** 
   * The only blob that has a constant name for a core is the metadata. Basically the Blob store's equivalent for a core
   * of the highest segments_N file for a Solr server. 
   */
  public static final String CORE_METADATA_BLOB_FILENAME = "core.metadata";
  
  public static String buildBlobStoreMetadataName(String suffix) {
    return CORE_METADATA_BLOB_FILENAME + "." + suffix;
  }

  /**
   * Generates a metadataSuffix value that gets appended to the name of {@link BlobCoreMetadata}
   * that are pushed to blob store
   */
  public static String generateMetadataSuffix() {
    return UUID.randomUUID().toString();
  }

  /***
   * syncLocalCoreWithSharedStore checks the local core has the latest copy stored in shared storage. Updates from the blob store always override the
   * local content.
   * @throws SolrException if the local core was not successfully sync'd.
   */
  public static void syncLocalCoreWithSharedStore(String collectionName, String coreName, String shardName, CoreContainer coreContainer,
                                                  SharedShardVersionMetadata shardVersionMetadata, boolean isLeaderPulling) throws SolrException {
    assert coreContainer.isZooKeeperAware();

    ZkController zkController = coreContainer.getZkController();
    DocCollection collection = zkController.getClusterState().getCollection(collectionName);
    CoreStorageClient blobClient = coreContainer.getSharedStoreManager().getBlobStorageProvider().getClient();
    log.info("sync initialized for collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);

    Slice shard = collection.getSlicesMap().get(shardName);
    if (shard != null) {
      try {
        String sharedStoreName = (String) shard.get(ZkStateReader.SHARED_SHARD_NAME);
        SharedCoreConcurrencyController concurrencyController = coreContainer.getSharedStoreManager().getSharedCoreConcurrencyController();
        if (SharedShardMetadataController.METADATA_NODE_DEFAULT_VALUE.equals(shardVersionMetadata.getMetadataSuffix())) {
          //no-op pull
          BlobCoreMetadata emptyBlobCoreMetadata = BlobCoreMetadataBuilder.buildEmptyCoreMetadata(sharedStoreName);
          concurrencyController.updateCoreVersionMetadata(collectionName, shardName, coreName, shardVersionMetadata, emptyBlobCoreMetadata, isLeaderPulling);
          log.info("sync successful, nothing to pull, collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);
          return;
        }
        concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.BlobPullStarted);
        try {
          // Get blob metadata
          String blobCoreMetadataName = BlobStoreUtils.buildBlobStoreMetadataName(shardVersionMetadata.getMetadataSuffix());
          BlobCoreMetadata blobstoreMetadata = blobClient.pullCoreMetadata(sharedStoreName, blobCoreMetadataName);
          if (null == blobstoreMetadata) {
            // Zookepeer and blob are out of sync, could be due to eventual consistency model in blob or something else went wrong.
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "cannot get core.metadata file from shared store, blobCoreMetadataName=" + blobCoreMetadataName +
                    " shard=" + shardName +
                    " collectionName=" + collectionName +
                    " sharedStoreName=" + sharedStoreName);
          } else if (blobstoreMetadata.getIsDeleted()) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "core.metadata file is marked deleted in shared store, blobCoreMetadataName=" + blobCoreMetadataName +
                    " shard=" + shardName +
                    " collectionName=" + collectionName +
                    " sharedStoreName=" + sharedStoreName);
          } else if (blobstoreMetadata.getIsCorrupt()) {
            log.warn("core.Metadata file is marked corrpt, skipping sync, collection=" + collectionName +
                " shard=" + shardName + " coreName=" + coreName + " sharedStoreName=" + sharedStoreName);
            return;
          }

          // Get local metadata + resolve with blob metadata
          ServerSideMetadata serverMetadata = new ServerSideMetadata(coreName, coreContainer);
          SharedMetadataResolutionResult resolutionResult = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobstoreMetadata);
          PushPullData pushPullData = new PushPullData.Builder()
              .setCollectionName(collectionName)
              .setShardName(shardName)
              .setCoreName(coreName)
              .setSharedStoreName(sharedStoreName)
              .build();

          if (resolutionResult.getFilesToPull().size() > 0) {
            BlobDeleteManager deleteManager = coreContainer.getSharedStoreManager().getBlobDeleteManager();
            CorePushPull cp = new CorePushPull(blobClient, deleteManager, pushPullData, resolutionResult, serverMetadata, blobstoreMetadata);
            cp.pullUpdateFromBlob(/* waitForSearcher */ true);
            concurrencyController.updateCoreVersionMetadata(pushPullData.getCollectionName(), pushPullData.getShardName(), pushPullData.getCoreName(),
                shardVersionMetadata, blobstoreMetadata, isLeaderPulling);
          } else {
            log.warn(String.format("Why there are no files to pull even when we do not match with the version in zk? collection=%s shard=%s core=%s",
                collectionName, shardName, coreName));
          }
        } finally {
          concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.BlobPullFinished);
        }
      } catch (Exception ex) {
        // wrap every thrown exception in a solr exception
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error occured pulling shard=" + shardName + " collection=" + collectionName + " from shared store " + ex);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Sync requested for unknown shard=" + shardName + " in collection=" + collectionName);
    }

  }

  /**
   * Returns the list of core properties that are needed to create a missing core corresponding
   * to provided {@code replica} of the {@code collection}. 
   */
  public static Map<String, String> getSharedCoreProperties(ZkStateReader zkStateReader, DocCollection collection, Replica replica) throws KeeperException {
    // "numShards" is another property that is found in core descriptors. But it is only set on the cores created at 
    // collection creation time. It is not part of cores created by addition of replicas/shards or shard splits.
    // Once set, it is not even kept in sync with latest number of shards. That initial value does not seem to have any 
    // purpose beyond collection creation nor does its persistence as core property. Therefore we do not put it in any 
    // of missing cores we create.

    Map<String, String> params = new HashMap<>();
    params.put(CoreDescriptor.CORE_COLLECTION, collection.getName());
    params.put(CoreDescriptor.CORE_NODE_NAME, replica.getName());
    params.put(CoreDescriptor.CORE_SHARD, collection.getShardId(replica.getNodeName(), replica.getCoreName()));
    params.put(CloudDescriptor.REPLICA_TYPE, Replica.Type.SHARED.name());
    String configName = zkStateReader.readConfigName(collection.getName());
    params.put(CollectionAdminParams.COLL_CONF, configName);
    return params;
  }
}
