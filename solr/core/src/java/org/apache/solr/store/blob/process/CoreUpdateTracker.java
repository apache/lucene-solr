package org.apache.solr.store.blob.process;

import org.apache.solr.store.blob.process.CorePusher.CorePushInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * Class to track local core updates that need pushing to Blob Store.<p>
 * Hooks in searchserver code call {@link #updatingCore} to add cores needing pushing to Blob store. The data is pulled by
 * {@link CorePusher#pushCoreToBlob}.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class CoreUpdateTracker {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /** Guarded by class monitor */
    private static CoreUpdateTracker INSTANCE = null;

    /**
     * Get the CoreUpdateTracker instance to track core updates. This can later be refactored if we need more than one.
     */
    public synchronized static CoreUpdateTracker get() {
        if (INSTANCE == null) {
            INSTANCE = new CoreUpdateTracker();
        }
        
        return INSTANCE;
    }

    /**
     * This method is called when a core is updated locally so it is pushed to Blob store at some later point
     */
    public void updatingCore(String coreName) {
    		try {
    			CorePushInfo corePushInfo = new CorePushInfo(coreName);
    			CorePusher.pushCoreToBlob(corePushInfo);
        } catch (InterruptedException ie) {
            // If we got here we likely haven't added the core to the list of cores to push, but if we got interrupted it
            // means the system is shutting down (otherwise no reason). So let the next blocking call handle that...
            // Not showing the strack trace of the interruption because it's not interesting, the cause would be interesting
            // but we don't have it.
            logger.warn("Core " + coreName + " not added to Blob push list. System shutting down?");
            Thread.currentThread().interrupt();
        }
    }
}
