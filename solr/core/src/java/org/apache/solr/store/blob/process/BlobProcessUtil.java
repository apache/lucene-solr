package org.apache.solr.store.blob.process;

import org.apache.solr.core.CoreContainer;

/**
 * Utils related to blob background process e.g. init/shutdown of blob's background processes
 *
 * @author mwaheed
 * @since 218/solr.7
 */
public class BlobProcessUtil {

    /**
     * Initialize background blob processes
     */
    public static void init(CoreContainer cores) {
        // Start the Blob store sync core push, async core pull, and blob file delete machinery
        CorePusher.init(cores);
        CorePullerFeeder.init(cores);
        BlobDeleteManager.init();
    }

    /**
     * Shutdown background blob processes
     */
    public static void shutdown() {
        // Stop the threads pulling cores from the Blob store
        CorePullerFeeder.shutdown();

        // And stop the threads managing blob physical delete for deleted blobs
        BlobDeleteManager.shutdown();
    }
}
