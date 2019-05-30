package org.apache.solr.store.blob.process;

import java.lang.invoke.MethodHandles;

import org.apache.lucene.index.IndexNotFoundException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.CorePushPull;
import org.apache.solr.store.blob.metadata.MetadataResolver;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.blob.metadata.ServerSideCoreMetadata;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes synchronous pushes of core updates to blob store. See the implementation of asynchronous pulls
 * in {@link CorePullerFeeder}.
 * 
 * Pushes will be triggered from {@link CoreUpdateTracker}, which Solr code notifies when cores have changed locally 
 * and need pushing to Blob store.
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
    static void pushCoreToBlob(PushPullData pushPullData) throws InterruptedException {
        if (coreContainer.isShutDown()) {
          // TODO include retry logic or record number of attempts somehow
          logger.warn("Tried to push a core update for core [" + pushPullData.getSharedStoreName() + "] but CoreContainer was already shut down.");
          return;
        }

        try {
            // Do the sequence of actions required to push a core to the Blob store.
            ServerSideCoreMetadata serverMetadata = new ServerSideCoreMetadata(pushPullData.getCoreName(), coreContainer);
            CoreStorageClient blobClient = BlobStorageProvider.get().getBlobStorageClient();
            
            BlobCoreMetadata blobMetadata = blobClient.pullCoreMetadata(pushPullData.getSharedStoreName());
            if (blobMetadata == null) {
                blobMetadata = BlobCoreMetadataBuilder.buildEmptyCoreMetadata(pushPullData.getSharedStoreName());
                logger.info("BlobCoreMetadata does not exist on the BlobStore. Pushing a new Metadata object.");
            }
            
            MetadataResolver resolver = new MetadataResolver(serverMetadata, blobMetadata);

            // Resolver message, can be null.
            final String message = resolver.getMessage();
            CoreSyncStatus statusToReportWithSuccessfulPush = CoreSyncStatus.SUCCESS;

            switch (resolver.getAction()) {
                case PUSH:
                  statusToReportWithSuccessfulPush = CoreSyncStatus.SUCCESS;
                  // Fall through to actually push the core to Blob
                  break;
                case PULL:
                  // Somehow the blob is fresher than we are, so not pushing anything to it
                  finishedPush(CoreSyncStatus.BLOB_FRESHER, message, pushPullData.getSharedStoreName());
                  return;
                case CONFIG_CHANGE:
                  // it is possible that config files to push are empty and config files to pull are non-empty
                  if(resolver.getConfigFilesToPush().isEmpty()){
                    finishedPush(CoreSyncStatus.SUCCESS_EQUIVALENT, message, pushPullData.getSharedStoreName());
                    return;
                  }
                  statusToReportWithSuccessfulPush = CoreSyncStatus.SUCCESS_CONFIG;
                  // Fall through to push config changes
                  break;
                case EQUIVALENT:
                  // Blob already got all that it needs. Possibly a previous task was delayed enough and pushed the
                  // changes enqueued twice (and we are the second task to run)
                  finishedPush(CoreSyncStatus.SUCCESS_EQUIVALENT, message, pushPullData.getSharedStoreName());
                  return;
                case CONFLICT:
                  // Well, this is the kind of things we hope do not occur too often. Unclear who wins here.
                  // TODO more work required to address this.
              		finishedPush(CoreSyncStatus.BLOB_CONFLICT, message, pushPullData.getSharedStoreName());
                  return;
                case BLOB_CORRUPT:
                  // Blob being corrupt at this stage should be pretty straightforward: remove whatever the blob has
                  // for the core and push our local version. Leaving this for later though
                  // TODO likely replace Blob content with local core
                  finishedPush(CoreSyncStatus.BLOB_CORRUPT, message, pushPullData.getSharedStoreName());
                  return;
                case BLOB_DELETED:
                  // Avoid pushing cores that are marked for delete in blob. The local copy will be eventually cleaned up by {@link OrphanCoreDeleter}
                  finishedPush(CoreSyncStatus.BLOB_DELETED_FOR_PUSH, message, pushPullData.getSharedStoreName());
                  return;
                default:
                  // Somebody added a value to the enum without saying?
                  logger.warn("Unexpected enum value " + resolver.getAction() + ", please update the code");
                  finishedPush(CoreSyncStatus.FAILURE, message, pushPullData.getSharedStoreName());
                  return;
            }

            CorePushPull cp = new CorePushPull(pushPullData, resolver, serverMetadata, blobMetadata);
            cp.pushToBlobStore();
            finishedPush(statusToReportWithSuccessfulPush, message, pushPullData.getSharedStoreName());
        } catch (InterruptedException e) {
            throw e;
        } catch (IndexNotFoundException infe) {
            finishedPush(CoreSyncStatus.LOCAL_MISSING_FOR_PUSH, null, pushPullData.getSharedStoreName());
            logger.warn("Failed to push core " + pushPullData.getCoreName() + " because no longer exists", infe);
        } catch (Exception e) {
            finishedPush(CoreSyncStatus.FAILURE, e.getMessage(), pushPullData.getSharedStoreName());
            logger.warn("Failed to push core " + pushPullData.getCoreName(), e);
        }
    }

    /** *
     * Log success or failure of core push.
     * @param status of push
     * @param message from metadata resolver
     * @param blobName of the core being pushed
     */
    public static void finishedPush(CoreSyncStatus status, String message, String blobName) {
        if (status.isSuccess()) {
            logger.info(String.format("Pushing core %s succeeded. Last status=%s. %s", 
                blobName, status, message == null ? "" : message));
        } else {
            logger.warn(String.format("Pushing core %s failed. Giving up. Last status=%s. %s", 
                blobName, status, message == null ? "" : message));
        }
    }
}