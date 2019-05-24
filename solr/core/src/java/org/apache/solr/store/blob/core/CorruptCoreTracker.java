package org.apache.solr.store.blob.core;

import com.force.commons.util.concurrent.NamedThreadFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import search.blobstore.client.CoreStorageClient;
import search.blobstore.solr.BlobCoreMetadata;

import org.apache.solr.core.CoreContainer;
import searchserver.blobstore.provider.BlobStorageProvider;
import searchserver.core.CoreDeleter;
import searchserver.core.CoreMetadata;
import searchserver.core.CoreMetadataProvider;
import searchserver.filter.LogContext;
import searchserver.logging.SearchLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.logging.Level;

/**
 * In-memory data structure tracking a per core state machine for core corruption handling.<p>
 * This state machine has to deal with multiple threads hitting corruption on the same core, yet costly operations such
 * as fetching the core from Blob Store or deleting the local core should not done by multiple threads in parallel.<p>
 *
 * There are two components of the per core state machine implemented here and tracked in {@link CorruptCoreTracker#corruptCores}:<ul>
 *     <li>{@link State}: which is the classical state a core is currently in. When a new corruption is encountered, the State
 *     method is executed while the lock on {@link CorruptCoreTracker#corruptCores} is held.</li>
 *     <li>{@link Action}: code executed either asynchronously after the end of processing done by the {@link State} method
 *     or synchronously when called from an execution Action, that does the actual verifications and transitions to a new
 *     State etc. Action code is executed without holding the local on the map, so if it needs to read or update the map
 *     it should acquire the lock first.</li>
 * </ul>
 *
 * There are a few entry ponts int this class:<ol>
 *     <li>{@link #corruptCoreSeen}: called when a Solr server encounters an issue (corruption) while opening or accessing
 *     a core</li>
 *     <li>{@link #corruptionOnPull}: called when an error occurs while pulling a core from Blob Store</li>
 *     <li>{@link #pulledBlob}: when a blob was successfully pulled from the Blob Store</li>
 *     <li>{@link #coreCommittedLocally}: when a core got a local commit, which means it diverges from the Blob Store version</li>
 *     <li>{@link #isPretendCoreCorrupt}: when Solr needs to check if a locally absent core should be considered as present
 *     but corrupt so that Core App does not try to reindex it (and do a fallback for example) because we're still waiting for
 *     some other server to update the blob store with a valid version that we could pull to resolve the corruption.</li>
 * </ol>
 *
 * @see <a href="https://docs.google.com/document/d/1QipZBMvdIKBYsykjE8WMUDo88KVicoKcXglWq8KyJVI/edit#heading=h.c33alnju0cpn">
 *     https://docs.google.com/document/d/1QipZBMvdIKBYsykjE8WMUDo88KVicoKcXglWq8KyJVI/edit#heading=h.c33alnju0cpn
 *     </a><br>
 * for a nicely drawn state machine that I'm unable to reproduce here in ASCII art.<p>
 *
 * @author iginzburg
 * @since 216/solr.6
 */
public class CorruptCoreTracker {
    private static final SearchLogger logger = new SearchLogger(CorruptCoreTracker.class);

    /**
     * When a corrupt core has been in the {@link State#REPAIR_CORE_DELETE} or {@link State#FINAL_CORE_DELETE}
     * states for longer than this duration, we consider something went wrong and restart deletion.
     */
    final static long MAX_CORE_DELETE_IN_PROGRESS_DELAY = TimeUnit.MINUTES.toMillis(3);

    /**
     * When a core is corrupt locally and we've pulled the Blob Store version and the core is still corrupt, this is the
     * minimal elapsed delay between checks in Blob Store (i.e. fetching Blob Store core metadata) to see if the core
     * there has been updated to give it another try.
     */
    final static long DELAY_BETWEEN_BLOB_CHECKS = TimeUnit.SECONDS.toMillis(10);

    /**
     * How long it should reasonably take to fetch the Blob metadata for a core to decide if the core should be deleted
     * again to trigger a new fetch or if we've made no progress towards resolution...<br>
     * This value is similar to {@link #MAX_CORE_DELETE_IN_PROGRESS_DELAY} in that it's here to restart an action that might
     * have failed for obscure reasons...
     */
    final static long MAX_BLOB_CHECK_DELAY = TimeUnit.MINUTES.toMillis(1);

    /**
     * Maximum time we'll wait for Blob core delete before considering it should be done again.
     */
    final static long BLOB_DELETE_DELAY = TimeUnit.MINUTES.toMillis(3);

    /**
     * When local core is corrupt and the Blob Store version of the core is corrupt as well, this is the maximum total
     * delay we accept waiting for another search server to put a new version of the core in the Blob Store (that we'd
     * then pull to see if the problem is solved) before giving up and deleting the local core as well as the Blob Store
     * core to trigger a reindexing by Core App and the creation of a new core.
     */
    final static long MAX_DELAY_WAITING_FOR_NEW_BLOB_DATA = TimeUnit.MINUTES.toMillis(2);

    private static Executor executor = Executors.newCachedThreadPool(new NamedThreadFactory("CorruptCoreTracker"));


    /**
     * Map of cores having been identified as corrupt.<p>
     * This map serves two purposes:
     * <ol>
     *     <li>Capture the state in the process of fixing a local core corruption (fetch from Blob first, if doesn't help delete core)</li>
     *     <li>Allow other threads hitting a corruption of the core to not duplicate the effort already started.</li>
     * </ol>
     * In order to protect the system against a thread supposed to fetch from Blob but failing to do so, the local core
     * will be deleted after a delay if the fetch from Blob did not happen.<p>
     *
     * Access guarded by itself.
     */
    private final Map<String, CorruptCoreInfo> corruptCores = new HashMap<>();


    /**
     * Method called when a corrupt core has been observed (the calling code got an exception and ended up here while handling
     * the exception).<p>
     * This method uses the local map and state machine to executeAsync the appropriate actions, advancing the state machine until
     * resolution for the core is reached. Resolution can take two forms:<ul>
     *     <li>The core ends up being deleted and eventually a request will notice that and will trigger the missing core handling,</li>
     *     <li>A valid version of the core ends up being pulled from Blob store and requests are happy again using the core.</li>
     * </ul>
     * Note that it wouldn't make sense at this stage to talk about reindexing/replay because the core is corrupt so would
     * not be accepting replay indexing :)
     */
    public void corruptCoreSeen(CoreContainer cores, String corename) {
        Action action;
        CorruptCoreInfo cci;
        long now = System.currentTimeMillis();
        synchronized (corruptCores) {
            cci = corruptCores.get(corename);

            // First time around.
            if (cci == null) {
                cci = new CCIBuilder(cores, corename, now).setState(State.REPAIR_CORE_DELETE).build();
                logStateChange(corruptCores, cci, "initial corruption reported on core");
                logger.log(Level.WARNING, null, "initial corruption reported on core", new Exception("for stack trace")); // TODO delete line
                corruptCores.put(corename, cci);
                action = Action.DO_REPAIR_DELETE;
            } else {
                action = cci.state.encounteredCorruption(corruptCores, cci, now);
            }
        }

        action.executeAsync(this, cci);
    }

    /**
     * This method is called when the pull from Blob store fails (after getting the core.metadata file, otherwise we don't
     * consider there was a pull). Use case targeted by this is the Blob Store core is corrupt and during the pull execution
     * when we (re) open the IndexWriter (call to openIndexWriter() in {@link searchserver.blobstore.metadata.CorePushPull#pullUpdateFromBlob()})
     * we get an exception.<p>
     *
     * If we failed on pull we record the version of the Blob Store core we tried to pull so we don't try again (unless
     * we've changed it locally since pulling), delete the local core and go wait for blob to get new content (or
     * eventually give up).
     *
     * <p>TODO make sure we separate issue due to bad files from issue due to bad network or Blob Store availability
     */
    public void corruptionOnPull(CoreContainer cores, String corename, @NonNull BlobCoreMetadata metadata) {
        final boolean deleteBlobOnPullFailure;

        // We delete the local core then wait for Blob Store to have content different from the content that caused the failure
        CorruptCoreInfo cci;
        CorruptCoreInfo newCci;
        CCIBuilder newCciBuilder;
        long now = System.currentTimeMillis();
        synchronized (corruptCores) {
            cci = corruptCores.get(corename);

            final String additionalInfo;
            // We build from existing entry in the map to preserve some past information.
            if (cci == null) {
                newCciBuilder = new CCIBuilder(cores, corename, now);
                additionalInfo = "Pull corruption notified on core previously not marked corrupt";
                deleteBlobOnPullFailure = false;
            } else {
                // The core was already tracked in the state machine, but regardless of what state it was in, we need to
                // delete it and make sure we don't attempt to pull same version from Blob again.
                // Note it is unlikely we were in the final delete states (FINAL_BLOB_DELETE and FINAL_CORE_DELETE) since
                // we got it when a pull was being attempted.
                newCciBuilder = new CCIBuilder(cci);
                additionalInfo = "Pull corruption notified on core already tracked for corruption";
                deleteBlobOnPullFailure = cci.state.deleteBlobOnPullFailure;
            }

            if (deleteBlobOnPullFailure) {
                // Issue on pull: mark the Blob Core as corrupt if the corruption was encountered on pulling a fresh complete
                // copy from the blob store. At the time of writing, this is only the case for State.DELETED_PULLABLE
                markBlobCorrupt(metadata);
            }


            newCci = newCciBuilder.setState(State.REPAIR_CORE_DELETE)
                    .setBlobCoreMetadata(metadata).setUniqueId(metadata.getUniqueIdentifier()).build();
            logStateChange(corruptCores, newCci, additionalInfo);
            corruptCores.put(corename, newCci);
        }

        Action.DO_REPAIR_DELETE.executeAsync(this, newCci);
    }

    /**
     * Called when any blob is pulled (ideally only when pulled from scratch, not when an update is pulled).
     * If the blob is in the corruption map, it means we've deleted it to try to pull again and see if it works, in which
     * case we should advance its state in the state machine.<p>
     *
     * If it is not in the map, then nothing to do.
     */
    public void pulledBlob(String corename, BlobCoreMetadata metadata) {
        synchronized (corruptCores) {
            CorruptCoreInfo cci = corruptCores.get(corename);

            // Is the core just loaded from blob being tracked as corrupt?
            if (cci == null) {
                // Blob is not in our tables so not considered corrupt, this is a normal core pull.
            } else if (cci.state == State.DELETED_PULLABLE) {
                // As we pull the blob we haven't indexed on it (yet) so committedLocally left to default false.
                CorruptCoreInfo newcci = new CCIBuilder(cci.cores, corename, cci.firstCorruptionTs)
                        .setState(State.BLOB_PULLED)
                        .resetLastBlobCheckTs()
                        .setBlobCoreMetadata(metadata)
                        .setUniqueId(metadata.getUniqueIdentifier())
                        .build();
                logStateChange(corruptCores, newcci, "core pulled from Blob");
                corruptCores.put(corename, newcci);
                // Now we have to wait until the core is being used. If it's still corrupt, we'll get calls into corruptCoreSeen()...
            } else if (cci.state == State.BLOB_PULLED){
                // This is the state in which cores that were fixed by pulling will stay until removed from the state machine.
                // If multiple pulls occur and no issues found we can likely remove the core from the CorruptCoreTracker.
                // On the other hand, note that a core in this state or a core not tracked in this class ends up moving to
                // the same state REPAIR_CORE_DELETE upon encountering a corruption, so it's ok to leave it here...
            } else {
                // Some race condition here or a restarted operation completing late. We don't expect pulls of "corrupt" (or so tracked)
                // cores to happen in any state other than DELETED_PULLABLE (removed core waiting to be pulled) or BLOB_PULLED
                // (previously pulled and then pulled again as/when needed and multiple times in case no corruptions occurred).
                logger.log(Level.WARNING, null, corename + " was pulled while in state=" + cci.state + " in CorruptCoreInfo.");
            }
        }
    }

    /**
     * Called for any core when a local commit is done. This allows tracking (for cores that are in the corruption map) that
     * the core has changed compared to the Blob version last pulled. This allows in case a corruption is detected on a Solr
     * server to know if the Blob version should be considered corrupt (got an error without doing local changes) or if
     * the core should be fetched again in case the problem was introduced locally.
     */
    public void coreCommittedLocally(String corename) {
        synchronized (corruptCores) {
            CorruptCoreInfo cci = corruptCores.get(corename);

            if (cci != null) {
                CorruptCoreInfo newcci = new CCIBuilder(cci).setCommittedLocally().build();
                corruptCores.put(corename, newcci);
            }
        }

    }

    /**
     * When a core is not present locally, this method allows checking if this is due to the way we handle core corruptions
     * by deleting the core first, pulling it from Blob Store later on. When a core is in this situation, the client
     * (Core App) should not automatically be told the core is missing (because then it would reindex the entity) but instead
     * an exception should be reported on accessing the core
     */
    public boolean isPretendCoreCorrupt(String corename) {
        CorruptCoreInfo cci;
        synchronized (corruptCores) {
            cci = corruptCores.get(corename);
        }

        // We get here because we didn't find the index directory for the core. If it's absent because we've deleted a corrupt
        // core but didn't give up on hope to retrieve a sane copy from Blob Store, return a corruption error to Core App so
        // it doesn't give up yet on that core.
        // If we're in a state where we want a new core to be pulled from Blob to test it, let Solr (SfdcDispatchFilter) pull
        // whatever is on Blob to see if we still have errors.
        return cci != null && cci.state != State.DELETED_PULLABLE;
    }

    /**
     * Logging only method, must be called with corruptCores lock held before the corruptCores map is updated.
     * TODO: eventually move logging OUTSIDE of the critical section
     */
    static private void logStateChange(Map<String, CorruptCoreInfo> corruptCores, CorruptCoreInfo cci, String additionalInfo) {
        CorruptCoreInfo oldCci = corruptCores.get(cci.corename);
        State oldState = oldCci == null ? null : oldCci.state;
        logger.info(/*Level.INFO, null,*/ "Change of state: " + cci.corename + " moving from state "
                + oldState + " to state " + cci.state + ". Reason: " + additionalInfo
                + " instance of corruptCores " + System.identityHashCode(corruptCores));
    }

    /**
     * Current state of a core that has been seen corrupt.<br>
     * Processing a given state has usually two consequences:<ul>
     *     <li>Updating the state for the core to the new state that is transitioned to,</li>
     *     <li>Executing the {@link Action} that does the work required in the new state.</li></ul>
     * Then, the new state will be executed when the next core corruption is encountered.<p>
     *     
     * While a state is in progress (or assumed to still be worked on by a previously started {@link Action} executed
     * asynchronously), the consequence of processing a state (i.e. of encountering a new core corruption) is a no-op:
     * no transition to a new state and no Action executed (by using {@link Action#DO_NOTHING}).<p>
     * 
     * This mechanism is put in place obviously to track and orchestrate the evolution of the resolution of a core
     * corruption but also to make sure required actions are not taken multiple times by multiple threads and generate
     * a high and useless load on local file system, Blob store etc.
     */
    enum State {

        /**
         * Delete (then delete in progress) the local core because a corruption was found.
         */
        REPAIR_CORE_DELETE(false) {
            @Override
            Action encounteredCorruption(Map<String, CorruptCoreInfo> corruptCores, CorruptCoreInfo currentInfo, long now) {
                long startedMsAgo = now - currentInfo.stateEntryTs;
                if (startedMsAgo <= MAX_CORE_DELETE_IN_PROGRESS_DELAY) {
                    // Wait hasn't been too long, so wait more
                    return Action.DO_NOTHING;
                } else {
                    // If waited for too long for delete to happen. Maybe the deleting thread got distracted? Try again.
                    logger.log(Level.WARNING, null, "DO_REPAIR_DELETE for " + currentInfo.corename + " started " + startedMsAgo + " ms ago and did not complete. Trying to delete again.");
                    // Refresh the the stateEntryTs by rebuilding an instance otherwise identical
                    CorruptCoreInfo reenter = new CCIBuilder(currentInfo).build();
                    logStateChange(corruptCores, reenter, "repair core delete took too long");
                    corruptCores.put(currentInfo.corename, reenter);
                    return Action.DO_REPAIR_DELETE;
                }
            }
        },

        /**
         * The core is no longer available locally and it might be absent for a while as we wait for Blob Store to get
         * a new version we could try. We don't want to keep polling Blob Store to see if it got updated so
         * there's a minimum delay between blob checks during which we do nothing and let all queries addressing this
         * core fail (but not with an unknown or missing core exception).<p>
         *
         * See code in {@link searchserver.filter.SfdcDispatchFilter#getCore(CoreContainer, String, LogContext)}<p>
         *
         * Note we need to return the appropriate error to the client AND we need to advance the state machine to perform
         * a blob check if time has come to check again.
         *
         */
        DELETED_CORRUPT(false) {
            @Override
            Action encounteredCorruption(Map<String, CorruptCoreInfo> corruptCores, CorruptCoreInfo currentInfo, long now) {
                long lastBlobCheckMsAgo = now - currentInfo.lastBlobCheckTs;
                // Upon initial entry into this state currentInfo.lastBlobCheckTs will be 0 or -1 and we will move to
                // State.BLOB_CHECK_IN_PROGRESS right away (i.e. on the second reported corruption, see Javadoc comment
                // of Action enum for more context)
                if (lastBlobCheckMsAgo <= DELAY_BETWEEN_BLOB_CHECKS) {
                    return Action.DO_NOTHING;
                } else {
                    // Go fetch the Blob Store metadata see if something might have changed there...
                    // Immediately setting the last blob check timestamp to prevent other threads from trying to start a check
                    // at the same time (can't wait for the check to have finished in doBlobCheck() to do that).
                    CorruptCoreInfo clone = new CCIBuilder(currentInfo).setState(State.CHECK_BLOB).resetLastBlobCheckTs().build();
                    logStateChange(corruptCores, clone, "about to check Blob store");
                    corruptCores.put(currentInfo.corename, clone);
                    return Action.DO_BLOB_CHECK;
                }
            }
        },

        /**
         * Checks if something has changed on Blob Store for the core. Also limits checking the Blob Store to a single thread.<p>
         *
         * The result of the check (in {@link Action#DO_BLOB_CHECK}) can have one of three outcomes:<ul>
         *     <li>No update on Blob, back to {@link #DELETED_CORRUPT} for a future new check of Blob,</li>
         *     <li>No update on Blob but it's been too long, move to {@link #FINAL_BLOB_DELETE} and start the delete process,</li>
         *     <li>Blob was updated (or is different from the local core), pull the Blob version then move to {@link #BLOB_PULLED}.</li>
         *     <li>Last but not least, if Blob content was already deleted, proceed to delete the local core in
         *         {@link #FINAL_CORE_DELETE}</li></ul><p>
         *
         * As always, if we see a core stuck in this state we do something so that it does not stay blocked forever.
         */
        CHECK_BLOB(false) {
            @Override
            Action encounteredCorruption(Map<String, CorruptCoreInfo> corruptCores, CorruptCoreInfo currentInfo, long now) {
                long startedMsAgo = now - currentInfo.stateEntryTs;
                if (startedMsAgo <= MAX_BLOB_CHECK_DELAY) {
                    // Wait hasn't been too long, so wait more
                    return Action.DO_NOTHING;
                } else {
                    // If waited for too long for blob check to happen, try again
                    logger.log(Level.WARNING, null, "Checking blob update for " + currentInfo.corename + " started " + startedMsAgo + " ms ago and did not complete. Starting check again.");
                    // Refresh the info since we're trying to check again (resets state entry timestamp)
                    CorruptCoreInfo reenter = new CCIBuilder(currentInfo).build();
                    logStateChange(corruptCores, reenter, "blob check took too long");
                    corruptCores.put(currentInfo.corename, reenter);
                    return Action.DO_BLOB_CHECK;
                }
            }
        },

        /**
         * The core has been deleted locally and Blob got a new version and we're waiting here for a request to pull
         * that new version. The transition from this state into {@link #BLOB_PULLED} is a special one, it is done
         * by code calling {@link #pulledBlob(String, BlobCoreMetadata)}.<p>
         *
         * The management that has to be done in this state is to handle cases where we observe this core state while
         * the core is not deleted (remember state code gets called when a core corruption is encountered). This is likely
         * possible given we can (by design) have race conditions on state transition when we restart states.<p>
         *
         * When we do encounter a corruption from this state we move to {@link #REPAIR_CORE_DELETE}, skipping
         * {@link #BLOB_PULLED}. This is not expected to happen very often.<p>
         *
         * This is the only state for which a corruption upon pulling a core from the Blob Store leads to marking the core
         * as corrupt in the Blob Store (pull corruptions in other states only delete the local copy of the core by
         * transitioning to state {@link State#REPAIR_CORE_DELETE}.
         */
        DELETED_PULLABLE(true) {
            @Override
            Action encounteredCorruption(Map<String, CorruptCoreInfo> corruptCores, CorruptCoreInfo currentInfo, long now) {
                // As notes in Javadoc above, we don't normally expect corrupt core handling for locally deleted cores,
                // but given our state tracking is not "strict", this could happen.
                return BLOB_PULLED.encounteredCorruption(corruptCores, currentInfo, now);
            }
        },


        /**
         * The Blob Store core got pulled into the local core (after we started to track the core due to a corruption),
         * so if a thread sees this it means the corruption is still not solved.<p>
         * We delete the local core (again) and retry the process (and will keep doing so as long as the Blob content changes).
         *
         *
         * DELETE BELOW
         * From here an action is required but there are two options:
         * <ul>
         *     <li>The version on the blob is identical to the one pulled, we should wait (in case it changes) and if it doesn't
         *     change, delete the local core and the Blob Store copy and let Core App request reindexing and new core creation.</li>
         *     <li>The version on the blob is different than the one previously pulled. We should have it pulled (by
         *     deleting local core) it and see if the issue is solved.</li>
         * </ul>
         *
         * Because we don't want all solr servers to go and delete the core (and request reindexing) immediately when an
         * issue is detected (because possibly another server that has correct data is able to push a new copy to Blob store
         * and solve the issue in a more elegant way), a server should wait between the last pull of a core and the decision
         * completely delete the core (if Blob Store wasn't updated by then).
         */
        BLOB_PULLED(false) {
            @Override
            Action encounteredCorruption(Map<String, CorruptCoreInfo> corruptCores, CorruptCoreInfo currentInfo, long now) {
                    // Delete the local core with the pulled Blob. Start all over.
                    CorruptCoreInfo clone = new CCIBuilder(currentInfo).setState(State.REPAIR_CORE_DELETE).build();
                    logStateChange(corruptCores, clone, "pulled blob encountered corruption");
                    corruptCores.put(currentInfo.corename, clone);
                    return Action.DO_REPAIR_DELETE;
            }
        },

        /**
         * Once we give up on the core being fixed, we delete the Blob store version of the core. Note this delete is somewhat
         * sticky, so no server is going to push back its local copy of the core to "undelete" the Blob version.
         */
        FINAL_BLOB_DELETE(false) {
            @Override
            Action encounteredCorruption(Map<String, CorruptCoreInfo> corruptCores, CorruptCoreInfo currentInfo, long now) {
                long startedMsAgo = now - currentInfo.stateEntryTs;
                if (startedMsAgo <= BLOB_DELETE_DELAY) {
                    // Wait hasn't been too long, so wait more
                    return Action.DO_NOTHING;
                } else {
                    // If waited for too long for blob delete to happen, try again
                    logger.log(Level.WARNING, null, "Blob store delete for " + currentInfo.corename + " started " + startedMsAgo + " ms ago and did not complete. Doing again.");
                    // Staying in same state but resetting the state entry timestamp
                    CorruptCoreInfo reenter = new CCIBuilder(currentInfo).build();
                    logStateChange(corruptCores, reenter, "final blob delete took too long");
                    corruptCores.put(currentInfo.corename, reenter);
                    return Action.DO_FINAL_BLOB_DELETE;
                }
            }
        },

        /**
         * After the Blob Store copy of the core got deleted (by us or by another server) we delete the local version of the
         * core.
         * This state is similar to {@link #REPAIR_CORE_DELETE}. The difference is what happens once the delete is done:
         * we're done and the core is no longer tracked as corrupt (it doesn't exist anymore) and it's removed from the tracking map.
         */
        FINAL_CORE_DELETE(false) {
            @Override
            Action encounteredCorruption(Map<String, CorruptCoreInfo> corruptCores, CorruptCoreInfo currentInfo, long now) {
                long startedMsAgo = now - currentInfo.stateEntryTs;
                if (startedMsAgo <= MAX_CORE_DELETE_IN_PROGRESS_DELAY) {
                    // Wait hasn't been too long, so wait more
                    return Action.DO_NOTHING;
                } else {
                    // If waited for too long for local core delete to happen, try again
                    logger.log(Level.WARNING, null, "Final core delete for " + currentInfo.corename + " started " + startedMsAgo + " ms ago and did not complete. Doing again.");
                    // Transitioning to same state but resetting the state entry timestamp in the process
                    CorruptCoreInfo reenter = new CCIBuilder(currentInfo).build();
                    logStateChange(corruptCores, reenter, "final core delete took too long");
                    corruptCores.put(currentInfo.corename, reenter);
                    return Action.DO_FINAL_CORE_DELETE;
                }
            }
        };

        /**
         * A pull failure can be due to the pulled content but also to content present on the local server making the pulled
         * content appear corrupt. That's why we're cautious before deleting the Core on Blob Store when {@link #corruptionOnPull}
         * gets called and only do so if we've deleted the local copy before the pull, i.e. if the current satte is
         * {@link State#DELETED_PULLABLE}.
         */
        private final boolean deleteBlobOnPullFailure;

        State(boolean deleteBlobOnPullFailure) {
            this.deleteBlobOnPullFailure = deleteBlobOnPullFailure;
        }


        /**
         * This method is called by a Solr thread notifying of an issue when opening or using a local core.
         * This method is (and must be!) called while holding a global lock (on <code>corruptCores</code>) so should not
         * block (i.e. no network access).<p>
         * This method can also have side effects of updating the <code>corruptCores</code> map.<p>
         */
        abstract Action encounteredCorruption(Map<String, CorruptCoreInfo> corruptCores, CorruptCoreInfo currentInfo, long now);
    }

    /**
     * The action to be done once the state machine update (running in the critical section) is done.<p>
     * Calling {@link #executeAsync} on the action calls the appropriate {@link CorruptCoreTracker} method.<p>
     *
     * There are two ways to execute actions:<ul>
     *     <li>When searchserver encounters a corruption with a core it calls into the {@link State} method of the core that
     *     asynchronously executes an {@link Action} (using {@link Action#executeAsync(CorruptCoreTracker, CorruptCoreInfo)}),</li>
     *     <li>When a previous action has completed, it can automatically execute the next action (after having updated
     *     the {@link State} of the core). Such calls are done synchronously using {@link Action#executeNow(CorruptCoreTracker, CorruptCoreInfo)}
     *     since the previous action already executes on a thread different from the searchserver thread having reported
     *     the corruption (being async from an already async execution doesn't add much)</li>
     * </ul>
     *
     * Note that executing an action or a sequence of actions update the {@link State} of the corrupt core as tracked in
     * the map {@link CorruptCoreTracker#corruptCores}. In case a sequence of actions fail at some stage, once new a corruption
     * are notified by searchserver code (to get a {@link State} executed and then the corresponding {@link Action}),
     * the actions will resume from the latest reached {@link State}.
     */
    enum Action {
        /**
         * Do nothing. Current state is being addressed by another thread.
         */
        DO_NOTHING(null),

        /**
         * Delete the local core then set its state to {@link State#DELETED_CORRUPT}.
         */
        DO_REPAIR_DELETE(CorruptCoreTracker::doRepairDelete),

        /**
         * Check if the version of the core on the Blob Store is different from the one we had locally ("had" because by the
         * time we get here we've already deleted it, but we kept track of what we had).<p>
         * If Blob has a different version, prepare to have it pulled on the next reported corruption by moving to {@link State#DELETED_PULLABLE}.<br>
         * If there's nothing new to pull from Blob, depending on how long we've already been waiting, either go back to
         * {@link State#DELETED_CORRUPT} to wait more, or move to state {@link State#FINAL_BLOB_DELETE} and
         * start right away to deletion of the core on the Blob Store (so that eventually Core App deals with a missing core
         * and fixes the issue by reindexing affected entities). If the core is no longer available on the Blob Store, proceed
         * directly to delete the local copy in {@link State#FINAL_CORE_DELETE}.
         */
        DO_BLOB_CHECK(CorruptCoreTracker::doBlobCheck),

        /**
         * We get here once we gave up on waiting for somebody to fix the core. We remove the Blob Store version of the core.
         */
        DO_FINAL_BLOB_DELETE(CorruptCoreTracker::doFinalBlobDelete),

        /**
         * We get here once we deleted the Blob Store version of the core and we do a final delete of the local copy of the core.
         * This will trigger a missing core exception next time Core App tries to talk to a search server
         * that deleted the core locally and Core App will reindex the data in another core.
         */
        DO_FINAL_CORE_DELETE(CorruptCoreTracker::doFinalCoreDelete);

        // Store the instance method to run async. First type is the instance to use since the method is an instance method.
        BiConsumer<CorruptCoreTracker, CorruptCoreInfo> method;

        Action(BiConsumer<CorruptCoreTracker, CorruptCoreInfo> instanceMethod) {
            this.method = instanceMethod;
        }

        /**
         * Runs SYNCHRONOUSLY (on the calling thread) the required corrective action to progress on resolving the core corruption
         */
        void executeNow(CorruptCoreTracker cct, CorruptCoreInfo cci) {
            // When method is null (Action.DO_NOTHING) we don't call executeNow :)
            this.method.accept(cct, cci);
        }

        /**
         * Runs async (on another thread) the required corrective actions (if any) to progress on resolving the core corruption
         */
        void executeAsync(CorruptCoreTracker cct, CorruptCoreInfo cci) {
            // null method means do nothing. Nothing is handled synchronously :)
            if (this.method != null) {
                executor.execute(new RunMethodAsync(this.method, cct, cci));
            }
        }

        /**
         * Class to call instance methods of {@link CorruptCoreTracker} asynchronously from {@link Action} instances.
         */
        private static class RunMethodAsync implements Runnable {
            final CorruptCoreTracker cct;
            final CorruptCoreInfo cci;
            final BiConsumer<CorruptCoreTracker, CorruptCoreInfo> method;

            RunMethodAsync(BiConsumer<CorruptCoreTracker, CorruptCoreInfo> method, CorruptCoreTracker cct, CorruptCoreInfo cci) {
                this.method = method;
                this.cct = cct;
                this.cci = cci;
            }

            @Override
            public void run() {
                method.accept(cct, cci);
            }
        }
    }

    /**
     * When this method is called the core would have already been set to state {@link State#REPAIR_CORE_DELETE}.
     * This method deletes the local (search server) core and once delete has completed sets the core tracking state to
     * {@link State#DELETED_CORRUPT}.
     */
    private void doRepairDelete(CorruptCoreInfo cci) {
        if (internalCoreDelete(cci, "CORE_DELETE_IN_PROGRESS state handling")) {
            // Now mark the state to show delete has completed. And start tracking the time waiting for Blob to update.
            CorruptCoreInfo newcci = new CCIBuilder(cci).setState(State.DELETED_CORRUPT).setStartBlobUpdateWaitTs(System.currentTimeMillis()).build();
            synchronized (corruptCores) {
                logStateChange(corruptCores, newcci, "successfully deleted local core for repair attempt");
                corruptCores.put(cci.corename, newcci);
            }
        }
        // Note if the local delete has failed we stay in State.REPAIR_CORE_DELETE state and it will be attempted again.
    }


    /**
     * Called when enough time has passed since last time we checked if there was an update on Blob Store for the core
     * (or if we've never checked it for that matter).
     * Checks if the Blob Store version of the core is a new one that would be worth checking locally see if it fixed the corruptions.<p>
     *
     * @see Action#DO_BLOB_CHECK
     */
    private void doBlobCheck(CorruptCoreInfo cci) {
        try {
            CoreStorageClient blobClient = BlobStorageProvider.get().getBlobStorageClient();
            BlobCoreMetadata blobMetadata = blobClient.pullCoreMetadata(cci.corename);

            if (blobMetadata == null || blobMetadata.getIsDeleted()) {
                // Nothing on blob or already deleted by another server or a previous async operation we started and that
                // we restarted because considered it lost/stuck for too long. Move on to deleting our local core for the last time.
                CorruptCoreInfo cciForFinalDelete = new CCIBuilder(cci).setState(State.FINAL_CORE_DELETE).setBlobCoreMetadata(blobMetadata).build();
                updateCorruptionDataAndLog(cci.corename, cciForFinalDelete, "core deleted or marked as such on Blob store");

                // Synchronously call the local core delete.
                Action.DO_FINAL_CORE_DELETE.executeNow(this, cciForFinalDelete);
            } else {
                // There's content on Blob Store. Fresher than what we have? If not, can we wait longer?

                // We pre-build the "pulled" state of the core that we might clone before using. This is a shortcut and the code
                // could be cleaned up later to directly build the required CoreCorruptInfo without going through an intermediate step.
                CCIBuilder newCciBuilder = new CCIBuilder(cci.cores, cci.corename, cci.firstCorruptionTs)
                        .setBlobCoreMetadata(blobMetadata)
                        .setUniqueId(blobMetadata.getUniqueIdentifier())
                        .resetLastBlobCheckTs(); // We could be copying cci.lastBlobCheckTs all the same here. Was set in DELETED_CORRUPT.encounteredCorruption()

                if (!blobMetadata.getIsCorrupt() && (cci.committedLocally || !blobMetadata.getUniqueIdentifier().equals(cci.uniqueId))) {
                    // Blob has changed (or we locally changed what we previously pulled from blob and might have corrupted
                    // it in the process) and is not marked as corrupt. We'll try to pull it (or pull it again) see if it fixes our issues
                    CorruptCoreInfo cciForDelete = newCciBuilder.setState(State.DELETED_PULLABLE).build();
                    updateCorruptionDataAndLog(cci.corename, cciForDelete, "Blob not marked corrupt and has content different from local content");
                    // Next Core App query hitting DELETED_PULLABLE will pull from Blob. We could reconsider and trigger pull now.
                } else {
                    // Blob hasn't changed (and we didn't index locally) so we know Blob is corrupt as well. Let others know
                    markBlobCorrupt(blobMetadata);

                    // Can we wait more for a Blob update or should we give up now?
                    if (System.currentTimeMillis() - cci.startBlobUpdateWaitTs <= MAX_DELAY_WAITING_FOR_NEW_BLOB_DATA) {
                        // We can wait more for another server to update and fix the Blob version of the core...
                        // Update the tracking of the corrupt core essentially to account for the blob check timestamp.
                        // Note we do not change startBlobUpdateWaitTs because we want to eventually stop waiting if Blob doesn't change.
                        CorruptCoreInfo newCci = newCciBuilder.setState(State.DELETED_CORRUPT).setStartBlobUpdateWaitTs(cci.startBlobUpdateWaitTs).build();
                        updateCorruptionDataAndLog(cci.corename, newCci, "no new content on Blob, waiting more");
                    } else {
                        // Wait has lasted too long. Time to clean up and move on.
                        // Delete Blob Store copy. Then we'll delete the local core.
                        CorruptCoreInfo cciForBlobDelete = newCciBuilder.setState(State.FINAL_BLOB_DELETE).build();
                        updateCorruptionDataAndLog(cci.corename, cciForBlobDelete, "no new content on Blob, proceeding to delete blob");

                        // Synchronously do the blob delete (that will synchronously do the core delete)
                        Action.DO_FINAL_BLOB_DELETE.executeNow(this, cciForBlobDelete);
                    }
                }
            }
        } catch (Exception e) {
            // The check for blob update will be done again next time there's an access (through State.BLOB_CHECK_IN_PROGRESS)
            logger.log(Level.WARNING, null, "doAsyncCheckBlobUpdated for " + cci.corename + " got exception accessing blob store. ", e);
        }
    }

    /**
     * Best effort to mark a blob as corrupt. If it doesn't work we just log.
     */
    private void markBlobCorrupt(BlobCoreMetadata blobMetadata) {
        try {
            if (!blobMetadata.getIsCorrupt()) {
                BlobCoreMetadata corruptMetadata = blobMetadata.getCorruptOf();
                BlobStorageProvider.get().getBlobStorageClient().pushCoreMetadata(blobMetadata.getCoreName(), corruptMetadata);
                logger.log(Level.WARNING, null, "markBlobCorrupt for " + blobMetadata.getCoreName()
                        + " marked core as corrupt.");
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, null, "markBlobCorrupt for " + blobMetadata.getCoreName()
                    + " failed to mark core as corrupt. ", e);
        }
    }

    /**
     * Update of the corruption state tracking map to be used from {@link Action}. Actions run without the monitor on the map being
     * held (as opposed to {@link State} so need to explicitly take it.
     */
    private void updateCorruptionDataAndLog(String corename, CorruptCoreInfo newData, String additionalInfo) {
        synchronized (corruptCores) {
            // TODO eventually log outside of the critical section
            logStateChange(corruptCores, newData, additionalInfo);
            corruptCores.put(corename, newData);
        }
    }

    /**
     * When this method is called the core tracking would have already been set to {@link State#FINAL_BLOB_DELETE}.
     * Deletes Blob store core.<p>
     *
     * If Blob delete successful, this method then moves the core to {@link State#FINAL_CORE_DELETE} and synchronously calls
     * {@link #doFinalCoreDelete(CorruptCoreInfo)}.
     */
    private void doFinalBlobDelete(CorruptCoreInfo cci) {
        try {
            CoreStorageClient blobClient = BlobStorageProvider.get().getBlobStorageClient();

            // We've marked our intention in the map (in doBlobCheck() or in State.DO_FINAL_BLOB_DELETE when we retry and maybe elsewhere),
            // now delete the Blob core copy.
            // Blob copy delete should leave some information behind for a while so that other servers do not
            // push the core again to Blob Store
            // TODO: check blob delete strategy and if compatible with this...
            BlobCoreMetadata deletedMetadata = cci.pulledCore.getDeletedOf();
            blobClient.pushCoreMetadata(cci.corename, deletedMetadata);

            // We now delete the local core (this should be the last time we delete it...)
            CorruptCoreInfo cciForBlobDelete = new CCIBuilder(cci).setState(State.FINAL_CORE_DELETE).build();
            updateCorruptionDataAndLog(cci.corename, cciForBlobDelete, "about to do core final delete");

            Action.DO_FINAL_CORE_DELETE.executeNow(this, cciForBlobDelete);
        } catch (Exception e) {
            // We will try again once the delay waiting for State.DO_FINAL_BLOB_DELETE to complete will expire.
            logger.log(Level.WARNING, null, "doFinalBlobDelete for " + cci.corename + " got exception accessing blob store. ", e);
        }
    }


    /**
     * When this method is called the core would have already been set to state {@link State#FINAL_CORE_DELETE} and the
     * core would have been deleted from Blob Store.<p>
     * Deletes local core copy then removes the core from the corrupted core tracking map.
     */
    private void doFinalCoreDelete(CorruptCoreInfo cci) {
        // Delete local core
        boolean deleted = internalCoreDelete(cci, "final local core delete");

        if (deleted) {
            // We're (finally) done with this corruption. Clear the map as we're done with this core forever.
            synchronized (corruptCores) {
                // Delete the local core. Blob version no longer exists (or has a deleted flag set) so will not be pulled.
                logger.log(Level.INFO, null, "Change of state: " + cci.corename + " in state "
                        + cci.state + ". Removing and done with that core.");
                corruptCores.remove(cci.corename);
            }

            // Show Splunk our happiness (since nobody else is around)
            logger.log(Level.INFO, null, "Deleted local copy of " + cci.corename
                    + " as final step of corruption handling. Total time since first corruption="
                    + (System.currentTimeMillis() - cci.firstCorruptionTs) + " ms");
        }

        // Delete didn't work? Staying in FINAL_CORE_DELETE_IN_PROGRESS and will try again on next corruption.
    }

    /**
     * This method deletes the local (search server) core.<p>
     *
     * This method is called by the two States that need to delete the local core copy: {@link State#REPAIR_CORE_DELETE}
     * and {@link State#FINAL_CORE_DELETE}.
     *
     * @return <code>true</code> if local delete has succeeded, <code>false</code> otherwise.
     */
    private boolean internalCoreDelete(CorruptCoreInfo cci, String additionalLoggingContext) {
        // Get core location from metadata core. We get a default location if core does not exist :/
        CoreMetadata.CoreLocation location = CoreMetadataProvider.get(cci.cores).getCoreLocation(cci.corename);

        // Does core actually exist? Note it feels this is taking a shortcut assuming core storage if File based. But this
        // shortcut already exists in searchserver code. If Solr.7 code cleans this up, we should change here too. Note
        // we don't actually need localtion. The test below can be removed, then it is only used for logging.
        if (location.getInstanceDirectory().exists()) {
            // Try to delete the core in 2 minutes max. Note this delay should logically be lower than MAX_CORE_DELETE_IN_PROGRESS_DELAY
            // so we can think of a thread blocked here as having finished before a new one tries to delete (makes it nicer
            // to think of)
            try {
                CoreDeleter.deleteCoreByName(cci.cores, cci.corename, 2, TimeUnit.MINUTES);
            } catch (Exception ex) {
                logger.log(Level.SEVERE, null, "CorruptCoreTracker unable to delete core " + cci.corename +
                        "(" + additionalLoggingContext + "): " + ex.getMessage());
                // If delete has failed, we leave the tracking map as CORE_DELETE_IN_PROGRESS. The delete will be attempted again.
                // Doesn't make sense to stop trying, because we get called only if there's activity on that core that is failing.
                return false;
            }

            logger.log(Level.WARNING, null, "CorruptCoreTracker deleted index directory "
                    + location.getInstanceDirectory() + " for corrupt core " + cci.corename + " (" + additionalLoggingContext + ")");
        } else {
            logger.log(Level.WARNING, null, "CorruptCoreTracker had nothing to delete, index directory "
                    + location.getInstanceDirectory() + " for corrupt core " + cci.corename + " does not exist"
                    + " (" + additionalLoggingContext + ")");
        }

        return true;
    }

    /**
     * Class capturing info related to a local core tracked as corrupt (even though it might have been fixed in the process).
     */
    private static class CorruptCoreInfo {
        final CoreContainer cores;

        final String corename;

        final State state;

        /**
         * Contains the Blob Store metadata of a core downloaded to replace the locally corrupt core, or <code>null</code>
         * if no Blob core was downloaded. This value is useful in case the downloaded Blob Store core is corrupt as well.
         */
        final BlobCoreMetadata pulledCore;

        /**
         * Time at which this core got inserted into the corrupt core map.
         */
        final long firstCorruptionTs;

        /**
         * Time at which we last checked the Blob Store metadata to see if the core was updated there.
         */
        final long lastBlobCheckTs;

        /**
         * Time at which we started a wait to see if Blob Store content for the core was changing (change pushed by another server)
         */
        final long startBlobUpdateWaitTs;

        /**
         * Value of {@link BlobCoreMetadata#getUniqueIdentifier()} of the <b>previous</b> blob pull done for this core (i.e.
         * not the current blob pull captured in field {@link #pulledCore}) or null if there was no previous pull (and there
         * might not even be a current pull).
         */
        final String uniqueId;

        /**
         * When a core (pulled from Blob for example) is indexed locally and then seen as corrupt,
         * it doesn't mean the blob version is corrupt. The corruption could have been introduced locally.
         * So we must make sure in such a case we'd be refetching the Blob version to check...
         */
        final boolean committedLocally;

        /**
         * Time at which the current instance of {@link CorruptCoreInfo} was created, i.e. when we entered into the current state.
         */
        final long stateEntryTs;


        CorruptCoreInfo(CoreContainer cores, String corename, State state, @Nullable BlobCoreMetadata pulledCore,
                        long firstCorruptionTs, long lastBlobCheckTs, long startBlobUpdateWaitTs,
                        @Nullable String uniqueId, boolean committedLocally) {
            assert state != State.BLOB_PULLED || pulledCore != null;
            assert state != null;

            this.cores = cores;
            this.corename = corename;
            this.state = state;
            this.pulledCore = pulledCore;
            this.firstCorruptionTs = firstCorruptionTs;
            this.lastBlobCheckTs = lastBlobCheckTs;
            this.startBlobUpdateWaitTs = startBlobUpdateWaitTs;
            this.uniqueId = uniqueId;
            this.committedLocally = committedLocally;

            this.stateEntryTs = System.currentTimeMillis();
        }
    }

    private static class CCIBuilder {
        final CoreContainer cores;
        final String corename;
        final long firstCorruptionTs;

        State state = null;
        BlobCoreMetadata pulledCore = null;
        long lastBlobCheckTs = -1L;
        long startBlobUpdateWaitTs = -1L;
        String uniqueId = null;
        boolean committedLocally = false;

        CCIBuilder(CorruptCoreInfo cci) {
            this.cores = cci.cores;
            this.corename = cci.corename;
            this.firstCorruptionTs = cci.firstCorruptionTs;
            this.state = cci.state;
            this.pulledCore = cci.pulledCore;
            this.lastBlobCheckTs = cci.lastBlobCheckTs;
            this.startBlobUpdateWaitTs = cci.startBlobUpdateWaitTs;
            this.uniqueId = cci.uniqueId;
            this.committedLocally = cci.committedLocally;
        }

        CCIBuilder(CoreContainer cores, String corename, long firstCorruptionTs) {
            this.cores = cores;
            this.corename = corename;
            this.firstCorruptionTs = firstCorruptionTs;
        }

        CCIBuilder setState(State state) {
            this.state = state;
            return this;
        }

        CCIBuilder setBlobCoreMetadata(BlobCoreMetadata pulledCore) {
            this.pulledCore = pulledCore;
            return this;
        }

        CCIBuilder resetLastBlobCheckTs() {
            this.lastBlobCheckTs = System.currentTimeMillis();
            return this;
        }

        CCIBuilder setStartBlobUpdateWaitTs(long startBlobUpdateWaitTs) {
            this.startBlobUpdateWaitTs = startBlobUpdateWaitTs;
            return this;
        }

        CCIBuilder setUniqueId(String uniqueId) {
            this.uniqueId = uniqueId;
            return this;
        }

        CCIBuilder setCommittedLocally() {
            this.committedLocally = true;
            return this;
        }

        CorruptCoreInfo build() {
            return new CorruptCoreInfo(cores, corename, state, pulledCore,
                    firstCorruptionTs, lastBlobCheckTs, startBlobUpdateWaitTs,
                    uniqueId, committedLocally);
        }
    }
}
