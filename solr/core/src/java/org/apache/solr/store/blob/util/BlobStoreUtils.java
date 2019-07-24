package org.apache.solr.store.blob.util;
import static org.junit.Assert.assertTrue;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.UUID;

import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.CorePushPull;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.blob.metadata.ServerSideMetadata;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil.SharedMetadataResolutionResult;
import org.apache.solr.store.blob.process.BlobDeleteManager;
import org.apache.solr.store.blob.process.CoreSyncStatus;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for BlobStore components
 */
public class BlobStoreUtils {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public static String buildBlobStoreMetadataName(String suffix) {
    return BlobStorageProvider.CORE_METADATA_BLOB_FILENAME + "." + suffix;
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
  public static void syncLocalCoreWithSharedStore(String collectionName, String coreName, String shardName, CoreContainer coreContainer) throws SolrException
  {
    assertTrue(coreContainer.isZooKeeperAware());

    ZkController zkController = coreContainer.getZkController();
    SharedShardMetadataController sharedMetadataController = coreContainer.getSharedStoreManager().getSharedShardMetadataController();
    DocCollection collection = zkController.getClusterState().getCollection(collectionName);
    CoreStorageClient blobClient = coreContainer.getSharedStoreManager().getBlobStorageProvider().getDefaultClient();
    log.info("sync intialized for collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);

    CoreSyncStatus syncStatus = CoreSyncStatus.FAILURE;

    Slice shard = collection.getSlicesMap().get(shardName);
    if (shard != null) {
      try {
        String sharedStoreName = (String)shard.get(ZkStateReader.SHARED_SHARD_NAME);
        // Fetch the latest metadata from ZK.
        // TODO: this can be optimized, depends on correct handling of leadership change.
        VersionedData data = sharedMetadataController.readMetadataValue(collectionName, shardName, false);

        Map<String, String> nodeUserData = (Map<String, String>) Utils.fromJSON(data.getData());
        String metadataSuffix = nodeUserData.get(SharedShardMetadataController.SUFFIX_NODE_NAME);
        if (SharedShardMetadataController.METADATA_NODE_DEFAULT_VALUE.equals(metadataSuffix)) {
          log.info("sync successful, nothing to pull, collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);
          return ;
        }
        // Get blob metadata
        String blobCoreMetadataName = BlobStoreUtils.buildBlobStoreMetadataName(metadataSuffix);
        BlobCoreMetadata blobstoreMetadata = blobClient.pullCoreMetadata(sharedStoreName, blobCoreMetadataName);
        if (null == blobstoreMetadata) {
          // Zookepeer and blob are out of sync, could be due to eventual consistency model in blob or something else went wrong.
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "cannot get core.metadata file from shared store, blobCoreMetadataName=" + blobCoreMetadataName +
              " shard=" + shardName +
              " collectionName=" + collectionName +
              " sharedStoreName=" + sharedStoreName );
        } else if (blobstoreMetadata.getIsDeleted()) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "core.metadata file is marked deleted in shared store, blobCoreMetadataName=" + blobCoreMetadataName +
              " shard=" + shardName +
              " collectionName=" + collectionName +
              " sharedStoreName=" + sharedStoreName );
        } else if (blobstoreMetadata.getIsCorrupt()) {
          log.warn("core.Metadata file is marked corrpt, skipping sync, collection=" + collectionName + 
              " shard=" + shardName + " coreName=" + coreName + " sharedStoreName=" + sharedStoreName );
          return ;
        }

        // Get local metadata + resolve with blob metadata
        ServerSideMetadata serverMetadata = new ServerSideMetadata(coreName, coreContainer);
        SharedMetadataResolutionResult resolutionResult = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobstoreMetadata);
        PushPullData pushPullData = new PushPullData.Builder()
            .setCollectionName(collectionName)
            .setShardName(shardName)
            .setCoreName(coreName)
            .setSharedStoreName(sharedStoreName)
            .setLastReadMetadataSuffix(metadataSuffix)
            .setNewMetadataSuffix(BlobStoreUtils.generateMetadataSuffix())
            .setZkVersion(data.getVersion())
            .build();

        if (resolutionResult.getFilesToPull().size() > 0) {

          BlobDeleteManager deleteManager = coreContainer.getSharedStoreManager().getBlobDeleteManager();
          CorePushPull cp = new CorePushPull(blobClient, deleteManager, pushPullData, resolutionResult, serverMetadata, blobstoreMetadata);
          cp.pullUpdateFromBlob(/* waitForSearcher */ true);
        } else {
          log.info("sync successful, nothing to pull for collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);
        }
      } catch (Exception ex) {
        // wrap every thrown exception in a solr exception
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error occured pulling shard=" + shardName + " collection=" + collectionName + " from shared store "+ ex);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Sync requested for unknown shard=" + shardName + " in collection=" + collectionName);
    }

  }


}
