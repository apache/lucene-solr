package org.apache.solr.store.blob.util;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrCore;

import com.google.common.base.Throwables;

import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.metadata.CorePushPull;
import org.apache.solr.store.blob.provider.BlobStorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * Utility class for BlobStore components
 *
 * @author a.vuong
 * @since 214/solr.6
 */
public class BlobStoreUtils {
    
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    
    /**
     * Refresh local core with updates in blob
     * 
     * @return true when local core is updated, false otherwise
     */
    public static boolean refreshLocalCore(SolrCore core, boolean waitForSearcher) throws SolrException {
        try {
            // Is blob aware of this core? 
            CoreStorageClient blobClient = BlobStorageProvider.get().getBlobStorageClient();
            BlobCoreMetadata blobMetadata = blobClient.pullCoreMetadata(core.getName());
            if(blobMetadata == null) {
                logger.info("No blob metadata found for " + core.getName());
                return false;
            }
            
            CorePushPull updateCore = new CorePushPull(core);
            if(updateCore.shouldPerformPull()) {
                // TODO:
                //  just like query path consider pull on separate thread and understand if something better is needed
                //  around concurrency control of multiple requests
                //      long running indexing request:
                //          A pull that takes more time than timeout of indexing request will abort in the middle and
                //          will come back and can potenitally get stuck in a cycle.
                //      multiple requests:
                //          Multiple requests will queue up inside pull method dir lock and eventually later requests will fail
                //          because of changed local dir contents.
                updateCore.pullUpdateFromBlob(waitForSearcher);
            }
            
            return updateCore.shouldPerformPull();
        } catch (Exception ex) {
            // Failed to pull updates from blob so notify client
            throw new SolrException(ErrorCode.SERVER_ERROR, "Exception while pulling latest updates from blob: " + Throwables.getStackTraceAsString(ex));
        }
    }

    /**
     * Helper function, used to tell if blob pulling is enabled
     * 
     * @return true if EnableBlob is true and EnableBlobBackgroundPulling is true
     */
    public static boolean isPullingEnabled() {
      // TODO : make configurable
      return true;
    }

    /**
     * Helper function, used to tell if blob pushing is enabled
     * 
     * @return true if EnableBlob is true and EnableBlobBackgroundPushing is true
     */
    public static boolean isPushingEnabled() {
      // TODO : make configurable
      return true;
    }
}
