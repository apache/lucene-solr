package org.apache.solr.store.blob.metadata;

import org.apache.solr.store.blob.core.CorruptCoreTracker;

/**
 * Class used as an entry point to in memory (lost upon server restart) audit trails (logs) of Solr Server to Blob Store interactions.<p>
 * This is used to make local decisions based not only on current status of a local core or on a returned status from
 * a Blob Store but also based on past events.<p>
 *
 * Simple example: when a local core is corrupt we want to pull the Blob copy to replace it, and we do. But if it is
 * still corrupt afterwards, we DON'T WANT to pull again the Blob copy... This class helps make such calls.<p>
 *
 * Another example: when we poll the Blob store for a given core to see if a pull is required, if it is not we can capture
 * the poll date in order not to poll again immediately aftewards but implement some strategy of delay between polls (if
 * that's how we eventually decide to implement this).
 *
 * @author iginzburg
 * @since 216/solr.6
 */
public class BlobTransientLog {
    private static BlobTransientLog INSTANCE = null;

    private final CorruptCoreTracker corruptCoreTracker;

    private BlobTransientLog() {
        corruptCoreTracker = new CorruptCoreTracker();
    }

    public synchronized static BlobTransientLog get() {
        if (INSTANCE == null) {
            INSTANCE = new BlobTransientLog();
        }

        return INSTANCE;
    }

    public CorruptCoreTracker getCorruptCoreTracker() {
        return corruptCoreTracker;
    }

}
