package org.apache.solr.store.blob.process;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to track local core updates that need pushing to Blob Store.
 */
public class CoreUpdateTracker {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Guarded by class monitor */
  private static CoreUpdateTracker INSTANCE = null;

  private SharedShardMetadataController shardSharedMetadataController; 

  private CoreUpdateTracker(CoreContainer coreContainer) {
    shardSharedMetadataController = coreContainer.getSharedStoreManager().getSharedShardMetadataController();
  }

  /**
   * Get the CoreUpdateTracker instance to track core updates. This can later be refactored if we need more than one.
   */
  public synchronized static CoreUpdateTracker get(CoreContainer coreContainer) {
    if (INSTANCE == null) {
      INSTANCE = new CoreUpdateTracker(coreContainer);
    }
    return INSTANCE;
  }

  /**
   * 
   * Persist the shard index data to the underlying durable shared storage provider using the specified
   * cluster snapshot.
   * 
   * The responsibility is to the caller using the shared store layer to propagate the desired cluster state
   * from which we'll validate the update request
   * 
   * @param clusterState the state of the cluster
   * @param collectionName name of the share-type collection that should persist its shard index data 
   * to a shared storage provider
   * @param shardName name of the shard that should be persisted
   * @param coreName the name of the core from which the update request is being processed
   */
  public void persistShardIndexToSharedStore(ClusterState clusterState, String collectionName, String shardName, String coreName)
      throws SolrException {
    DocCollection collection = clusterState.getCollection(collectionName);
    if (!collection.getSharedIndex()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can't push shard index data for a collection that"
          + "is not of type shared. Collection=" + collectionName + " shard=" + shardName);
    }

    Slice shard = collection.getSlicesMap().get(shardName);
    if (shard != null) {
      try {
        if (!collection.getActiveSlices().contains(shard)) {
          // unclear if there are side effects but logging for now
          logger.warn("Performing a push for shard " + shardName + " that is inactive!");
        }
        logger.info("Initiating push for collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);
        // creates the metadata node if it doesn't exist
        shardSharedMetadataController.ensureMetadataNodeExists(collectionName, shardName);

        /*
         * Get the metadataSuffix value from ZooKeeper or from a cache if an entry exists for the 
         * given collection and shardName. If the leader has already changed, the conditional update
         * later will fail and invalidate the cache entry if it exists. 
         */
        VersionedData data = shardSharedMetadataController.readMetadataValue(collectionName, shardName, 
            /* readFromCache */ true);

        Map<String, String> nodeUserData = (Map<String, String>) Utils.fromJSON(data.getData());
        String metadataSuffix = nodeUserData.get(SharedShardMetadataController.SUFFIX_NODE_NAME);

        String sharedShardName = (String) shard.get(ZkStateReader.SHARED_SHARD_NAME);

        PushPullData pushPullData = new PushPullData.Builder()
            .setCollectionName(collectionName)
            .setShardName(shardName)
            .setCoreName(coreName)
            .setSharedStoreName(sharedShardName)
            .setLastReadMetadataSuffix(metadataSuffix)
            .setNewMetadataSuffix(BlobStoreUtils.generateMetadataSuffix())
            .setZkVersion(data.getVersion())
            .build();
        CorePusher.pushCoreToBlob(pushPullData);
      } catch (Exception ex) {
        // wrap every thrown exception in a solr exception
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error trying to push to blob store", ex);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can't push shard index data with name " + shardName 
          + " for collection " + collectionName + " because the shard does not exist in the cluster state");
    }
  }
}
