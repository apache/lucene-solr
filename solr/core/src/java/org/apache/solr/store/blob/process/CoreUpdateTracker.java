package org.apache.solr.store.blob.process;

import java.lang.invoke.MethodHandles;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to track local core updates that need pushing to Blob Store.
 */
public class CoreUpdateTracker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CoreContainer coreContainer;
  private SharedShardMetadataController shardSharedMetadataController; 

  public CoreUpdateTracker(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    shardSharedMetadataController = coreContainer.getSharedStoreManager().getSharedShardMetadataController();
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
          log.warn("Performing a push for shard " + shardName + " that is inactive!");
        }
        log.info("Initiating push for collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);

        String sharedShardName = (String) shard.get(ZkStateReader.SHARED_SHARD_NAME);

        PushPullData pushPullData = new PushPullData.Builder()
            .setCollectionName(collectionName)
            .setShardName(shardName)
            .setCoreName(coreName)
            .setSharedStoreName(sharedShardName)
            .build();
        CorePusher pusher = new CorePusher(coreContainer);
        pusher.pushCoreToBlob(pushPullData);
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
