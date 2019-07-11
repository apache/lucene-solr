package org.apache.solr.store.blob.process;

import java.lang.invoke.MethodHandles;

import org.apache.lucene.index.IndexNotFoundException;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.CorePushPull;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.blob.metadata.ServerSideMetadata;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil.SharedMetadataResolutionResult;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes synchronous pushes of core updates to blob store. See the implementation of asynchronous pulls
 * in {@link CorePullerFeeder}.
 * 
 * Pushes will be triggered from {@link CoreUpdateTracker}, which Solr code notifies when a shard's index data has 
 * changed locally and needs to be persisted to a shared store (blob store). 
 */
public class CorePusher {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static CoreContainer coreContainer;
  
  public static void init(CoreContainer coreContainer) {
    CorePusher.coreContainer = coreContainer;
    logger.info("EnableBlobBackgroundPushing is false, started CorePusher");
  }
    
  /**
   * Pushes the local core updates to the Blob store and logs whether the push succeeded or failed.
   */
  static void pushCoreToBlob(PushPullData pushPullData) throws Exception {
    BlobStorageProvider blobProvider = coreContainer.getZkController().getBlobStorageProvider(); 
    CoreStorageClient blobClient = blobProvider.getDefaultClient(); // TODO, use a real client
    BlobDeleteManager deleteManager = coreContainer.getZkController().getBlobDeleteManager(); // TODO, use a real client
        
    BlobCoreMetadata blobCoreMetadata = null;
    logger.info("Push to shared store initiating with PushPullData= " + pushPullData.toString());
    
    // Read the metadata file from shared store if this isn't the first push of this index shard
    try {
      if (!pushPullData.getLastReadMetadataSuffix().equals(
          SharedShardMetadataController.METADATA_NODE_DEFAULT_VALUE)) {
  
        String blobCoreMetadataName = BlobStoreUtils.buildBlobStoreMetadataName(pushPullData.getLastReadMetadataSuffix());
        blobCoreMetadata = blobClient.pullCoreMetadata(pushPullData.getSharedStoreName(), blobCoreMetadataName);
  
        if (blobCoreMetadata == null) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The shard index " + pushPullData.getSharedStoreName() + 
              " is missing for shard " + pushPullData.getShardName() + " for collection " + pushPullData.getCollectionName() + 
              " using metadataSuffix " + pushPullData.getLastReadMetadataSuffix());
        }
      } else {
        blobCoreMetadata = BlobCoreMetadataBuilder.buildEmptyCoreMetadata(pushPullData.getSharedStoreName());
        logger.info("This is the first time that shard " + pushPullData.getShardName() + " for collection " + 
            pushPullData.getCollectionName() + " is getting pushed to blob store.");
      }
      
      // Resolve the differences between the local shard index data and shard index data on shared store
      // if there is any
      ServerSideMetadata localShardMetadata = new ServerSideMetadata(pushPullData.getCoreName(), coreContainer);
      SharedMetadataResolutionResult resolutionResult = SharedStoreResolutionUtil.resolveMetadata(
          localShardMetadata, blobCoreMetadata);
      
      // begin the push process 
      CorePushPull pushPull = new CorePushPull(blobClient, deleteManager, pushPullData, resolutionResult, localShardMetadata, blobCoreMetadata);
      pushPull.pushToBlobStore();
      
      // at this point we've pushed the new metadata file with the newMetadataSuffix and now need to write to zookeeper
      SharedShardMetadataController shardSharedMetadataController = coreContainer.getZkController().getSharedShardMetadataController(); 
      shardSharedMetadataController.updateMetadataValueWithVersion(pushPullData.getCollectionName(), pushPullData.getShardName(),
          pushPullData.getNewMetadataSuffix(), pushPullData.getZkVersion());
      logger.info("Successfully pushed to shared store");
      
      // TODO - make error handling a little nicer?
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CorePusher was interrupted while pushing to blob store", e);
    } catch (IndexNotFoundException infe) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CorePusher failed because the core " + pushPullData.getCoreName() +
        " for the shard " + pushPullData.getShardName() + " was not found", infe);
    } catch (SolrException e) {
      Throwable t = e.getCause();
      if (t instanceof BadVersionException) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CorePusher failed to push because the node "
            + "version doesn't match. This shard is no longer the leader.", t); 
      }
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CorePusher failed to push shard index for " 
          + pushPullData.getShardName() + " due to unexpected exception", e);
    }
  }
}