package org.apache.solr.store.blob.process;

import java.util.Map;
import java.util.logging.Level;

import searchserver.SfdcConfig;
import searchserver.SfdcConfigProperty;
import searchserver.blobstore.process.CorePusherFeeder.PushCoreInfo;
import searchserver.blobstore.process.CorePusherFeeder.PushCoreInfoMerger;
import searchserver.blobstore.util.DeduplicatingList;
import searchserver.logging.SearchLogger;

/**
 * Class to track local core updates that need pushing to Blob Store.<p>
 * Hooks in searchserver code call {@link #updatingCore} to add cores needing pushing to Blob store. The data is pulled by
 * {@link CorePusherFeeder#feedTheMonsters} to be fed to threads pushing to the Blob store.<p>
 * This class provides the Blob store equivalent of Replication's {@link searchserver.replication.ReplicationManagerImpl#updatingCore}
 * but the problem is simpler here since we don't need to track changes by timestamp as replication does...<p>
 *
 * Another implementation option is for the hooks to directly be calling a method in {@link CorePusherFeeder} and
 * insert directly into the {@link DeduplicatingList} managed there. But that other list manages retries counting for
 * example so keeping the two lists separate. The one in this class has to be sized very large so that we never block Solr threads.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class CoreUpdateTracker {
    private static final SearchLogger logger = new SearchLogger(CoreUpdateTracker.class);

    /** Guarded by class monitor */
    private static CoreUpdateTracker INSTANCE = null;

    /**
     * Variable to quickly check if each update notification should trigger an enqueue for core push to Blob.
     */
    private static volatile boolean enqueueForPush = Boolean.parseBoolean(SfdcConfig.get().getSfdcConfigProperty(SfdcConfigProperty.EnableBlobBackgroundPushing));

    /**
     * The max size (in number of entries) of {@link #updatedCores}. Because that list is deduplicated on core name,
     * this is the maximum number of pending updates to different cores the system can handle without blocking the threads
     * calling {@link #updatingCore}. Basically we never want to block these threads...<p>
     * Note the corresponding Replication implementation is in {@link searchserver.replication.ReplicationManagerImpl#updatingCore}
     * that uses a {@link Map} that is not bounded in size. So having an unreasonably large max size here is reasonable.
     * (the other option would have been always pass reenqueue as <code>true</code> when calling
     * {@link DeduplicatingList#addDeduplicated(DeduplicatingList.Deduplicatable, boolean)} so size it not checked).
     */
    static private final int TRACKING_LIST_MAX_SIZE = 500000;
    private final DeduplicatingList<String, PushCoreInfo> updatedCores;

    /**
     * Get the CoreUpdateTracker instance to track core updates. This can later be refactored if we need more than one.
     */
    public synchronized static CoreUpdateTracker get() {
        if (INSTANCE == null) {
            INSTANCE = new CoreUpdateTracker();
        }

        // We refresh the config because it can be set by tests. In prod configs do not change once the JVM has started.
        // This relies on tests running sequentially, but if tests start running in parallel we'll have other issues anyway.
        //
        // In tests, ideally the config should be reloaded as each SearchServerWithContainer is started. We would then
        // store the various flags we use in the code per server (and not statically). This is not how things are done
        // looking at existing code (for example ReplicationManagerProvider.init(), SfdcQueryComponent.MAX_CONCURRENT_EXPENSIVE_QUERIES
        // and some others) so the three realistic options are 1. load once and never let it change (not great for tests),
        // 2. load from config each time the value is needed (performance impact?) and 3. try to be smart and reload when
        // it changes...
        // We attempt to be smart here and reload when needed yet caching the value for most accesses :)
        enqueueForPush = Boolean.parseBoolean(SfdcConfig.get().getSfdcConfigProperty(SfdcConfigProperty.EnableBlobBackgroundPushing));

        return INSTANCE;
    }

    private CoreUpdateTracker() {
        updatedCores = new DeduplicatingList<>(TRACKING_LIST_MAX_SIZE, new PushCoreInfoMerger());
    }



    /**
     * This method is called when a core is updated locally so it is pushed to Blob store at some later point
     */
    public void updatingCore(String coreName) {
        // Be as undisruptive as possible when Blob push is disabled.
        if (!enqueueForPush) {
            return;
        }

        PushCoreInfo pci = new PushCoreInfo(coreName);

        try {
            updatedCores.addDeduplicated(pci, false);
        } catch (InterruptedException ie) {
            // If we got here we likely haven't added the core to the list of cores to push, but if we got interrupted it
            // means the system is shutting down (otherwise no reason). So let the next blocking call handle that...
            // Not showing the strack trace of the interruption because it's not interesting, the cause would be interesting
            // but we don't have it.
            logger.log(Level.WARNING, null, "Core " + coreName + " not added to Blob push list. System shutting down?");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the next core to push to Blob store. This method can block waiting for a core that need pushing.
     */
    public PushCoreInfo getCoreToPush() throws InterruptedException {
        return updatedCores.removeFirst();
    }
}
