package org.apache.solr.store.blob.core;


import com.google.common.base.Throwables;
import edu.umd.cs.findbugs.annotations.NonNull;
import search.blobstore.solr.BlobCoreMetadata;

import org.apache.solr.core.CoreContainer;
import searchserver.blobstore.metadata.BlobTransientLog;
import searchserver.logging.SearchLogger;

import java.util.logging.Level;

/**
 * Class dealing with a corrupt solr core detected on the search server as well as corruptions detected on a core stored
 * on the blob store (these corruptions might in some cases only be detected once the core is pulled to the solr server).<p>
 *
 * This class is related to {@link searchserver.core.CoreDeleter} that handles core corruptions in a replication based context.
 *
 * @author iginzburg
 * @since 216/solr.6
 */
public class CorruptCoreHandler {
    private static final SearchLogger logger = new SearchLogger(CorruptCoreHandler.class);

    /**
     * Method to be called when a local core has encountered an exception, while being open or when executing a request.
     * This method plays a role similar to {@link searchserver.core.CoreDeleter#asyncDeleteCore} but the actions that need
     * to take place are more complex and the code manages multiple threads hitting a given issue at the same time.
     */
    public static void notifyOrLogCoreCorruption(CoreContainer cores, String corename, boolean fatalException, Throwable t, String ctx) {
        if (!fatalException) {
            logger.log(Level.INFO, null,
                    String.format("CorruptCoreHandler received non fatal exception embedded inside %s. Doing nothing. core=%s, type=%s, message=%s, stacktrace=%s",
                            ctx, corename, t.getClass().getCanonicalName(), t.getMessage(), Throwables.getStackTraceAsString(t)));
        } else {
            // Core is in bad shape and should (eventually) be fixed. Deal with it.
            BlobTransientLog.get().getCorruptCoreTracker().corruptCoreSeen(cores, corename);
        }

    }

    /**
     * Method to be called when pull from Blob Store has failed for a core because of the actual content (not because of
     * network connection or Blob Store unavailability).
     * Note that for this method to be called, the core metadata from the Blob store must have been fetched successfully.
     */
    public static void notifyBlobPullFailure(CoreContainer cores, String corename, @NonNull BlobCoreMetadata metadata) {
        assert metadata != null;
        assert metadata.getUniqueIdentifier() != null;

        BlobTransientLog.get().getCorruptCoreTracker().corruptionOnPull(cores, corename, metadata);
    }
}
