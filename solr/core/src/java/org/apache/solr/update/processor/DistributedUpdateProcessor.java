/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.SolrCmdDistributor.Error;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.update.VersionBucket;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

// NOT mt-safe... create a new processor for each add thread
// TODO: we really should not wait for distrib after local? unless a certain replication factor is asked for
public class DistributedUpdateProcessor extends UpdateRequestProcessor {

  final static String PARAM_WHITELIST_CTX_KEY = DistributedUpdateProcessor.class + "PARAM_WHITELIST_CTX_KEY";
  public static final String DISTRIB_FROM_SHARD = "distrib.from.shard";
  public static final String DISTRIB_FROM_COLLECTION = "distrib.from.collection";
  public static final String DISTRIB_FROM_PARENT = "distrib.from.parent";
  public static final String DISTRIB_FROM = "distrib.from";
  public static final String DISTRIB_INPLACE_PREVVERSION = "distrib.inplace.prevversion";
  protected static final String TEST_DISTRIB_SKIP_SERVERS = "test.distrib.skip.servers";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Request forwarded to a leader of a different shard will be retried up to this amount of times by default
   */
  static final int MAX_RETRIES_ON_FORWARD_DEAULT = Integer.getInteger("solr.retries.on.forward",  25);
  /**
   * Requests from leader to it's followers will be retried this amount of times by default
   */
  static final int MAX_RETRIES_TO_FOLLOWERS_DEFAULT = Integer.getInteger("solr.retries.to.followers", 3);

  /**
   * Values this processor supports for the <code>DISTRIB_UPDATE_PARAM</code>.
   * This is an implementation detail exposed solely for tests.
   * 
   * @see DistributingUpdateProcessorFactory#DISTRIB_UPDATE_PARAM
   */
  public static enum DistribPhase {
    NONE, TOLEADER, FROMLEADER;

    public static DistribPhase parseParam(final String param) {
      if (param == null || param.trim().isEmpty()) {
        return NONE;
      }
      try {
        return valueOf(param);
      } catch (IllegalArgumentException e) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST, "Illegal value for " + 
           DISTRIB_UPDATE_PARAM + ": " + param, e);
      }
    }
  }

  public static final String COMMIT_END_POINT = "commit_end_point";
  public static final String LOG_REPLAY = "log_replay";

  // used to assert we don't call finish more than once, see finish()
  private boolean finished = false;

  protected final SolrQueryRequest req;
  protected final SolrQueryResponse rsp;
  private final AtomicUpdateDocumentMerger docMerger;

  private final UpdateLog ulog;
  @VisibleForTesting
  VersionInfo vinfo;
  private final boolean versionsStored;
  private boolean returnVersions;

  private NamedList<Object> addsResponse = null;
  private NamedList<Object> deleteResponse = null;
  private NamedList<Object> deleteByQueryResponse = null;
  private CharsRefBuilder scratch;

  private final SchemaField idField;

  // these are setup at the start of each request processing
  // method in this update processor
  protected boolean isLeader = true;
  protected boolean forwardToLeader = false;
  protected boolean isSubShardLeader = false;
  protected boolean isIndexChanged = false;

  /**
   * Number of times requests forwarded to some other shard's leader can be retried
   */
  protected final int maxRetriesOnForward = MAX_RETRIES_ON_FORWARD_DEAULT;
  /**
   * Number of times requests from leaders to followers can be retried
   */
  protected final int maxRetriesToFollowers = MAX_RETRIES_TO_FOLLOWERS_DEFAULT;

  protected UpdateCommand updateCommand;  // the current command this processor is working on.

  protected final Replica.Type replicaType;

  public DistributedUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp,
    UpdateRequestProcessor next) {
    this(req, rsp, new AtomicUpdateDocumentMerger(req), next);
  }

  /** Specification of AtomicUpdateDocumentMerger is currently experimental.
   * @lucene.experimental
   */
  public DistributedUpdateProcessor(SolrQueryRequest req,
      SolrQueryResponse rsp, AtomicUpdateDocumentMerger docMerger,
      UpdateRequestProcessor next) {
    super(next);
    this.rsp = rsp;
    this.docMerger = docMerger;
    this.idField = req.getSchema().getUniqueKeyField();
    this.req = req;
    this.replicaType = computeReplicaType();
    // version init

    this.ulog = req.getCore().getUpdateHandler().getUpdateLog();
    this.vinfo = ulog == null ? null : ulog.getVersionInfo();
    versionsStored = this.vinfo != null && this.vinfo.getVersionField() != null;
    returnVersions = req.getParams().getBool(UpdateParams.VERSIONS ,false);

    // TODO: better way to get the response, or pass back info to it?
    // SolrRequestInfo reqInfo = returnVersions ? SolrRequestInfo.getRequestInfo() : null;

    // this should always be used - see filterParams
    DistributedUpdateProcessorFactory.addParamToDistributedRequestWhitelist
      (this.req,
       UpdateParams.UPDATE_CHAIN,
       TEST_DISTRIB_SKIP_SERVERS,
       CommonParams.VERSION_FIELD,
       UpdateParams.EXPUNGE_DELETES,
       UpdateParams.OPTIMIZE,
       UpdateParams.MAX_OPTIMIZE_SEGMENTS,
       UpdateParams.REQUIRE_PARTIAL_DOC_UPDATES_INPLACE,
       ShardParams._ROUTE_);

    //this.rsp = reqInfo != null ? reqInfo.getRsp() : null;
  }

  /**
   *
   * @return the replica type of the collection.
   */
  protected Replica.Type computeReplicaType() {
    return Replica.Type.NRT;
  }

  boolean isLeader() {
    return isLeader;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {

    assert TestInjection.injectFailUpdateRequests();

    setupRequest(cmd);

    // If we were sent a previous version, set this to the AddUpdateCommand (if not already set)
    if (!cmd.isInPlaceUpdate()) {
      cmd.prevVersion = cmd.getReq().getParams().getLong(DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION, -1);
    }
    // TODO: if minRf > 1 and we know the leader is the only active replica, we could fail
    // the request right here but for now I think it is better to just return the status
    // to the client that the minRf wasn't reached and let them handle it    

    boolean dropCmd = false;
    if (!forwardToLeader) {
      dropCmd = versionAdd(cmd);
    }

    if (dropCmd) {
      // TODO: do we need to add anything to the response?
      return;
    }

    doDistribAdd(cmd);

    // TODO: what to do when no idField?
    if (returnVersions && rsp != null && idField != null) {
      if (addsResponse == null) {
        addsResponse = new NamedList<>(1);
        rsp.add("adds",addsResponse);
      }
      if (scratch == null) scratch = new CharsRefBuilder();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      addsResponse.add(scratch.toString(), cmd.getVersion());
    }

    // TODO: keep track of errors?  needs to be done at a higher level though since
    // an id may fail before it gets to this processor.
    // Given that, it may also make sense to move the version reporting out of this
    // processor too.

  }

  protected void doDistribAdd(AddUpdateCommand cmd) throws IOException {
    // no-op for derived classes to implement
  }

  // must be synchronized by bucket
  private void doLocalAdd(AddUpdateCommand cmd) throws IOException {
    super.processAdd(cmd);
    isIndexChanged = true;
  }

  // must be synchronized by bucket
  private void doLocalDelete(DeleteUpdateCommand cmd) throws IOException {
    super.processDelete(cmd);
    isIndexChanged = true;
  }

  public static int bucketHash(BytesRef idBytes) {
    assert idBytes != null;
    return Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);
  }

  /**
   * @return whether or not to drop this cmd
   * @throws IOException If there is a low-level I/O error.
   */
  protected boolean versionAdd(AddUpdateCommand cmd) throws IOException {
    BytesRef idBytes = cmd.getIndexedId();

    if (idBytes == null) {
      super.processAdd(cmd);
      return false;
    }

    if (vinfo == null) {
      if (AtomicUpdateDocumentMerger.isAtomicUpdate(cmd)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Atomic document updates are not supported unless <updateLog/> is configured");
      } else {
        super.processAdd(cmd);
        return false;
      }
    }

    // This is only the hash for the bucket, and must be based only on the uniqueKey (i.e. do not use a pluggable hash
    // here)
    int bucketHash = bucketHash(idBytes);

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find any existing version in the document
    // TODO: don't reuse update commands any more!
    long versionOnUpdate = cmd.getVersion();

    if (versionOnUpdate == 0) {
      SolrInputField versionField = cmd.getSolrInputDocument().getField(CommonParams.VERSION_FIELD);
      if (versionField != null) {
        Object o = versionField.getValue();
        versionOnUpdate = o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString());
      } else {
        // Find the version
        String versionOnUpdateS = req.getParams().get(CommonParams.VERSION_FIELD);
        versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
      }
    }

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;
    boolean forwardedFromCollection = cmd.getReq().getParams().get(DISTRIB_FROM_COLLECTION) != null;

    VersionBucket bucket = vinfo.bucket(bucketHash);

    long dependentVersionFound = -1;
    // if this is an in-place update, check and wait if we should be waiting for a previous update (on which
    // this update depends), before entering the synchronized block
    if (!leaderLogic && cmd.isInPlaceUpdate()) {
      dependentVersionFound = waitForDependentUpdates(cmd, versionOnUpdate, isReplayOrPeersync, bucket);
      if (dependentVersionFound == -1) {
        // it means the document has been deleted by now at the leader. drop this update
        return true;
      }
    }

    vinfo.lockForUpdate();
    try {
      long finalVersionOnUpdate = versionOnUpdate;
      return bucket.runWithLock(vinfo.getVersionBucketLockTimeoutMs(), () -> doVersionAdd(cmd, finalVersionOnUpdate, isReplayOrPeersync, leaderLogic, forwardedFromCollection, bucket));
    } finally {
      vinfo.unlockForUpdate();
    }
  }

  private boolean doVersionAdd(AddUpdateCommand cmd, long versionOnUpdate, boolean isReplayOrPeersync,
      boolean leaderLogic, boolean forwardedFromCollection, VersionBucket bucket) throws IOException {
    try {
      BytesRef idBytes = cmd.getIndexedId();
      bucket.signalAll();
      // just in case anyone is waiting let them know that we have a new update
      // we obtain the version when synchronized and then do the add so we can ensure that
      // if version1 < version2 then version1 is actually added before version2.

      // even if we don't store the version field, synchronizing on the bucket
      // will enable us to know what version happened first, and thus enable
      // realtime-get to work reliably.
      // TODO: if versions aren't stored, do we need to set on the cmd anyway for some reason?
      // there may be other reasons in the future for a version on the commands

      if (versionsStored) {

        long bucketVersion = bucket.highest;

        if (leaderLogic) {

          if (forwardedFromCollection && ulog.getState() == UpdateLog.State.ACTIVE) {
            // forwarded from a collection but we are not buffering so strip original version and apply our own
            // see SOLR-5308
            if (log.isInfoEnabled()) {
              log.info("Removing version field from doc: {}", cmd.getPrintableId());
            }
            cmd.solrDoc.remove(CommonParams.VERSION_FIELD);
            versionOnUpdate = 0;
          }

          getUpdatedDocument(cmd, versionOnUpdate);

          // leaders can also be in buffering state during "migrate" API call, see SOLR-5308
          if (forwardedFromCollection && ulog.getState() != UpdateLog.State.ACTIVE
              && isReplayOrPeersync == false) {
            // we're not in an active state, and this update isn't from a replay, so buffer it.
            if (log.isInfoEnabled()) {
              log.info("Leader logic applied but update log is buffering: {}", cmd.getPrintableId());
            }
            cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
            ulog.add(cmd);
            return true;
          }

          if (versionOnUpdate != 0) {
            Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
            long foundVersion = lastVersion == null ? -1 : lastVersion;
            if (versionOnUpdate == foundVersion || (versionOnUpdate < 0 && foundVersion < 0)
                || (versionOnUpdate == 1 && foundVersion > 0)) {
              // we're ok if versions match, or if both are negative (all missing docs are equal), or if cmd
              // specified it must exist (versionOnUpdate==1) and it does.
            } else {
              if(cmd.getReq().getParams().getBool(CommonParams.FAIL_ON_VERSION_CONFLICTS, true) == false) {
                return true;
              }

              throw new SolrException(ErrorCode.CONFLICT, "version conflict for " + cmd.getPrintableId()
                  + " expected=" + versionOnUpdate + " actual=" + foundVersion);
            }
          }

          long version = vinfo.getNewClock();
          cmd.setVersion(version);
          cmd.getSolrInputDocument().setField(CommonParams.VERSION_FIELD, version);
          bucket.updateHighest(version);
        } else {
          // The leader forwarded us this update.
          cmd.setVersion(versionOnUpdate);

          if (shouldBufferUpdate(cmd, isReplayOrPeersync, ulog.getState())) {
            // we're not in an active state, and this update isn't from a replay, so buffer it.
            cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
            ulog.add(cmd);
            return true;
          }

          if (cmd.isInPlaceUpdate()) {
            long prev = cmd.prevVersion;
            Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
            if (lastVersion == null || Math.abs(lastVersion) < prev) {
              // this was checked for (in waitForDependentUpdates()) before entering the synchronized block.
              // So we shouldn't be here, unless what must've happened is:
              // by the time synchronization block was entered, the prev update was deleted by DBQ. Since
              // now that update is not in index, the vinfo.lookupVersion() is possibly giving us a version
              // from the deleted list (which might be older than the prev update!)
              UpdateCommand fetchedFromLeader = fetchFullUpdateFromLeader(cmd, versionOnUpdate);

              if (fetchedFromLeader instanceof DeleteUpdateCommand) {
                if (log.isInfoEnabled()) {
                  log.info("In-place update of {} failed to find valid lastVersion to apply to, and the document was deleted at the leader subsequently."
                      , idBytes.utf8ToString());
                }
                versionDelete((DeleteUpdateCommand) fetchedFromLeader);
                return true;
              } else {
                assert fetchedFromLeader instanceof AddUpdateCommand;
                // Newer document was fetched from the leader. Apply that document instead of this current in-place
                // update.
                if (log.isInfoEnabled()) {
                  log.info(
                      "In-place update of {} failed to find valid lastVersion to apply to, forced to fetch full doc from leader: {}",
                      idBytes.utf8ToString(), fetchedFromLeader);
                }
                // Make this update to become a non-inplace update containing the full document obtained from the
                // leader
                cmd.solrDoc = ((AddUpdateCommand) fetchedFromLeader).solrDoc;
                cmd.prevVersion = -1;
                cmd.setVersion((long) cmd.solrDoc.getFieldValue(CommonParams.VERSION_FIELD));
                assert cmd.isInPlaceUpdate() == false;
              }
            } else {
              if (lastVersion != null && Math.abs(lastVersion) > prev) {
                // this means we got a newer full doc update and in that case it makes no sense to apply the older
                // inplace update. Drop this update
                log.info("Update was applied on version: {}, but last version I have is: {}. Dropping current update"
                    , prev, lastVersion);
                return true;
              } else {
                // We're good, we should apply this update. First, update the bucket's highest.
                if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
                  bucket.updateHighest(versionOnUpdate);
                }
              }
            }
          } else {
            // if we aren't the leader, then we need to check that updates were not re-ordered
            if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
              // we're OK... this update has a version higher than anything we've seen
              // in this bucket so far, so we know that no reordering has yet occurred.
              bucket.updateHighest(versionOnUpdate);
            } else {
              // there have been updates higher than the current update. we need to check
              // the specific version for this id.
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
                // This update is a repeat, or was reordered. We need to drop this update.
                if (log.isDebugEnabled()) {
                  log.debug("Dropping add update due to version {}", idBytes.utf8ToString());
                }
                return true;
              }
            }
          }
          if (!isSubShardLeader && replicaType == Replica.Type.TLOG && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
            cmd.setFlags(cmd.getFlags() | UpdateCommand.IGNORE_INDEXWRITER);
          }
        }
      }

      SolrInputDocument clonedDoc = shouldCloneCmdDoc() ? cmd.solrDoc.deepCopy(): null;

      // TODO: possibly set checkDeleteByQueries as a flag on the command?
      doLocalAdd(cmd);

      if (clonedDoc != null) {
        cmd.solrDoc = clonedDoc;
      }
    } finally {
      bucket.unlock();
    }
    return false;
  }

  /**
   *
   * @return whether cmd doc should be cloned before localAdd
   */
  protected boolean shouldCloneCmdDoc() {
    return false;
  }

  @VisibleForTesting
  boolean shouldBufferUpdate(AddUpdateCommand cmd, boolean isReplayOrPeersync, UpdateLog.State state) {
    if (state == UpdateLog.State.APPLYING_BUFFERED
        && !isReplayOrPeersync
        && !cmd.isInPlaceUpdate()) {
      // this a new update sent from the leader, it contains whole document therefore it won't depend on other updates
      return false;
    }

    return state != UpdateLog.State.ACTIVE && isReplayOrPeersync == false;
  }

  /**
   * This method checks the update/transaction logs and index to find out if the update ("previous update") that the current update
   * depends on (in the case that this current update is an in-place update) has already been completed. If not,
   * this method will wait for the missing update until it has arrived. If it doesn't arrive within a timeout threshold,
   * then this actively fetches from the leader.
   * 
   * @return -1 if the current in-place should be dropped, or last found version if previous update has been indexed.
   */
  private long waitForDependentUpdates(AddUpdateCommand cmd, long versionOnUpdate,
                               boolean isReplayOrPeersync, VersionBucket bucket) throws IOException {
    long lastFoundVersion = 0;
    TimeOut waitTimeout = new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    vinfo.lockForUpdate();
    try {
      lastFoundVersion = bucket.runWithLock(vinfo.getVersionBucketLockTimeoutMs(), () -> doWaitForDependentUpdates(cmd, versionOnUpdate, isReplayOrPeersync, bucket, waitTimeout));
    } finally {
      vinfo.unlockForUpdate();
    }

    if (Math.abs(lastFoundVersion) > cmd.prevVersion) {
      // This must've been the case due to a higher version full update succeeding concurrently, while we were waiting or
      // trying to index this partial update. Since a full update more recent than this partial update has succeeded,
      // we can drop the current update.
      if (log.isDebugEnabled()) {
        log.debug("Update was applied on version: {}, but last version I have is: {} . Current update should be dropped. id={}"
            , cmd.prevVersion, lastFoundVersion, cmd.getPrintableId());
      }
      return -1;
    } else if (Math.abs(lastFoundVersion) == cmd.prevVersion) {
      assert 0 < lastFoundVersion : "prevVersion " + cmd.prevVersion + " found but is a delete!";
      if (log.isDebugEnabled()) {
        log.debug("Dependent update found. id={}", cmd.getPrintableId());
      }
      return lastFoundVersion;
    }

    // We have waited enough, but dependent update didn't arrive. Its time to actively fetch it from leader
    if (log.isInfoEnabled()) {
      log.info("Missing update, on which current in-place update depends on, hasn't arrived. id={}, looking for version={}, last found version={}",
          cmd.getPrintableId(), cmd.prevVersion, lastFoundVersion);
    }
    
    UpdateCommand missingUpdate = fetchFullUpdateFromLeader(cmd, versionOnUpdate);
    if (missingUpdate instanceof DeleteUpdateCommand) {
      if (log.isInfoEnabled()) {
        log.info("Tried to fetch document {} from the leader, but the leader says document has been deleted. Deleting the document here and skipping this update: Last found version: {}, was looking for: {}"
            , cmd.getPrintableId(), lastFoundVersion, cmd.prevVersion);
      }
      versionDelete((DeleteUpdateCommand) missingUpdate);
      return -1;
    } else {
      assert missingUpdate instanceof AddUpdateCommand;
      if (log.isDebugEnabled()) {
        log.debug("Fetched the document: {}", ((AddUpdateCommand) missingUpdate).getSolrInputDocument());
      }
      versionAdd((AddUpdateCommand)missingUpdate);
      if (log.isInfoEnabled()) {
        log.info("Added the fetched document, id= {}, version={}"
            , ((AddUpdateCommand) missingUpdate).getPrintableId(), missingUpdate.getVersion());
      }
    }
    return missingUpdate.getVersion();
  }

  private long doWaitForDependentUpdates(AddUpdateCommand cmd, long versionOnUpdate, boolean isReplayOrPeersync, VersionBucket bucket,
      TimeOut waitTimeout) {
    long lastFoundVersion;
    try {
      Long lookedUpVersion = vinfo.lookupVersion(cmd.getIndexedId());
      lastFoundVersion = lookedUpVersion == null ? 0L : lookedUpVersion;

      if (Math.abs(lastFoundVersion) < cmd.prevVersion) {
        if (log.isDebugEnabled()) {
          log.debug("Re-ordered inplace update. version={}, prevVersion={}, lastVersion={}, replayOrPeerSync={}, id={}",
              (cmd.getVersion() == 0 ? versionOnUpdate : cmd.getVersion()), cmd.prevVersion, lastFoundVersion,
              isReplayOrPeersync, cmd.getPrintableId());
        }
      }

      while (Math.abs(lastFoundVersion) < cmd.prevVersion && !waitTimeout.hasTimedOut()) {
        long timeLeftInNanos = waitTimeout.timeLeft(TimeUnit.NANOSECONDS);
        if(timeLeftInNanos > 0) { // 0 means: wait forever until notified, but we don't want that.
          bucket.awaitNanos(timeLeftInNanos);
        }
        lookedUpVersion = vinfo.lookupVersion(cmd.getIndexedId());
        lastFoundVersion = lookedUpVersion == null ? 0L : lookedUpVersion;
      }
    } finally {
      bucket.unlock();
    }
    return lastFoundVersion;
  }

  /**
   * This method is used when an update on which a particular in-place update has been lost for some reason. This method
   * sends a request to the shard leader to fetch the latest full document as seen on the leader.
   * @return AddUpdateCommand containing latest full doc at shard leader for the given id, or null if not found.
   */
  private UpdateCommand fetchFullUpdateFromLeader(AddUpdateCommand inplaceAdd, long versionOnUpdate) throws IOException {
    String id = inplaceAdd.getIndexedIdStr();
    UpdateShardHandler updateShardHandler = inplaceAdd.getReq().getCore().getCoreContainer().getUpdateShardHandler();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(DISTRIB, false);
    params.set("getInputDocument", id);
    params.set("onlyIfActive", true);
    SolrRequest<SimpleSolrResponse> ur = new GenericSolrRequest(METHOD.GET, "/get", params);

    String leaderUrl = getLeaderUrl(id);

    if(leaderUrl == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Can't find document with id=" + id);
    }

    NamedList<Object> rsp;
    try {
      ur.setBasePath(leaderUrl);
      rsp = updateShardHandler.getUpdateOnlyHttpClient().request(ur);
    } catch (SolrServerException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error during fetching [" + id +
          "] from leader (" + leaderUrl + "): ", e);
    }
    Object inputDocObj = rsp.get("inputDocument");
    Long version = (Long)rsp.get("version");
    SolrInputDocument leaderDoc = (SolrInputDocument) inputDocObj;

    if (leaderDoc == null) {
      // this doc was not found (deleted) on the leader. Lets delete it here as well.
      DeleteUpdateCommand del = new DeleteUpdateCommand(inplaceAdd.getReq());
      del.setIndexedId(inplaceAdd.getIndexedId());
      del.setId(inplaceAdd.getIndexedId().utf8ToString());
      del.setVersion((version == null || version == 0)? -versionOnUpdate: version);
      return del;
    }

    AddUpdateCommand cmd = new AddUpdateCommand(req);
    cmd.solrDoc = leaderDoc;
    cmd.setVersion((long)leaderDoc.getFieldValue(CommonParams.VERSION_FIELD));
    return cmd;
  }
  
  // TODO: may want to switch to using optimistic locking in the future for better concurrency
  // that's why this code is here... need to retry in a loop closely around/in versionAdd
  boolean getUpdatedDocument(AddUpdateCommand cmd, long versionOnUpdate) throws IOException {
    if (!AtomicUpdateDocumentMerger.isAtomicUpdate(cmd)) return false;

    if (idField == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Can't do atomic updates without a schema uniqueKeyField");
    }

    BytesRef rootIdBytes = cmd.getIndexedId(); // root doc; falls back to doc ID if no _route_
    String rootDocIdString = cmd.getIndexedIdStr();

    Set<String> inPlaceUpdatedFields = AtomicUpdateDocumentMerger.computeInPlaceUpdatableFields(cmd);
    if (inPlaceUpdatedFields.size() > 0) { // non-empty means this is suitable for in-place updates
      if (docMerger.doInPlaceUpdateMerge(cmd, inPlaceUpdatedFields)) {
        return true;
      } // in-place update failed, so fall through and re-try the same with a full atomic update
    }

    // if this is an atomic update, and hasn't already been done "in-place",
    // but the user indicated it must be done in palce, then fail with an error...
    if (cmd.getReq().getParams().getBool(UpdateParams.REQUIRE_PARTIAL_DOC_UPDATES_INPLACE, false)) {
      throw new SolrException
        (ErrorCode.BAD_REQUEST,
         "Can not satisfy '" + UpdateParams.REQUIRE_PARTIAL_DOC_UPDATES_INPLACE +
         "'; Unable to update doc in-place: " + cmd.getPrintableId());
    }
    
    // full (non-inplace) atomic update

    final SolrInputDocument oldRootDocWithChildren =
        RealTimeGetComponent.getInputDocument(
            req.getCore(),
            rootIdBytes,
            rootIdBytes,
            null,
            null,
            RealTimeGetComponent.Resolution.ROOT_WITH_CHILDREN); // when no children, just fetches the doc

    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    SolrInputDocument mergedDoc;
    if (oldRootDocWithChildren == null) {
      if (versionOnUpdate > 0
          || !rootDocIdString.equals(cmd.getChildDocIdStr())) {
        if (cmd.getReq().getParams().getBool(CommonParams.FAIL_ON_VERSION_CONFLICTS, true)
            == false) {
          return false;
        }
        // could just let the optimistic locking throw the error
        throw new SolrException(ErrorCode.CONFLICT, "Document not found for update.  id=" + rootDocIdString);
      }
      // create a new doc by default if an old one wasn't found
      mergedDoc = docMerger.merge(sdoc, new SolrInputDocument(idField.getName(), rootDocIdString));
    } else {
      oldRootDocWithChildren.remove(CommonParams.VERSION_FIELD);

      mergedDoc = docMerger.merge(sdoc, oldRootDocWithChildren);
    }

    cmd.solrDoc = mergedDoc;
    return true;
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    
    assert TestInjection.injectFailUpdateRequests();

    updateCommand = cmd;

    if (!cmd.isDeleteById()) {
      doDeleteByQuery(cmd);
    } else {
      doDeleteById(cmd);
    }
  }

  // Implementing min_rf here was a bit tricky. When a request comes in for a delete by id to a replica that does _not_
  // have any documents specified by those IDs, the request is not forwarded to any other replicas on that shard. Thus
  // we have to spoof the replicationTracker and set the achieved rf to the number of active replicas.
  //
  protected void doDeleteById(DeleteUpdateCommand cmd) throws IOException {

    setupRequest(cmd);

    boolean dropCmd = false;
    if (!forwardToLeader) {
      dropCmd  = versionDelete(cmd);
    }

    if (dropCmd) {
      // TODO: do we need to add anything to the response?
      return;
    }

    doDistribDeleteById(cmd);

    // cmd.getIndexId == null when delete by query
    // TODO: what to do when no idField?
    if (returnVersions && rsp != null && cmd.getIndexedId() != null && idField != null) {
      if (deleteResponse == null) {
        deleteResponse = new NamedList<>(1);
        rsp.add("deletes",deleteResponse);
      }
      if (scratch == null) scratch = new CharsRefBuilder();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      deleteResponse.add(scratch.toString(), cmd.getVersion());  // we're returning the version of the delete.. not the version of the doc we deleted.
    }
  }

  /**
   * This method can be overridden to tamper with the cmd after the localDeleteById operation
   * @param cmd the delete command
   * @throws IOException in case post processing failed
   */
  protected void doDistribDeleteById(DeleteUpdateCommand cmd) throws IOException {
    // no-op for derived classes to implement
  }

  /** @see DistributedUpdateProcessorFactory#addParamToDistributedRequestWhitelist */
  @SuppressWarnings("unchecked")
  protected ModifiableSolrParams filterParams(SolrParams params) {
    ModifiableSolrParams fparams = new ModifiableSolrParams();
    
    Set<String> whitelist = (Set<String>) this.req.getContext().get(PARAM_WHITELIST_CTX_KEY);
    assert null != whitelist : "whitelist can't be null, constructor adds to it";

    for (String p : whitelist) {
      passParam(params, fparams, p);
    }
    return fparams;
  }

  private void passParam(SolrParams params, ModifiableSolrParams fparams, String param) {
    String[] values = params.getParams(param);
    if (values != null) {
      for (String value : values) {
        fparams.add(param, value);
      }
    }
  }

  /**
   * for implementing classes to setup request data(nodes, replicas)
   * @param cmd the delete command being processed
   */
  protected void doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    // even in non zk mode, tests simulate updates from a leader
    setupRequest(cmd);
    doDeleteByQuery(cmd, null, null);
  }

  /**
   * should be called by implementing class after setting up replicas
   * @param cmd delete command
   * @param replicas list of Nodes replicas to pass to {@link DistributedUpdateProcessor#doDistribDeleteByQuery(DeleteUpdateCommand, List, DocCollection)}
   * @param coll the collection in zookeeper {@link org.apache.solr.common.cloud.DocCollection},
   *             passed to {@link DistributedUpdateProcessor#doDistribDeleteByQuery(DeleteUpdateCommand, List, DocCollection)}
   */
  protected void doDeleteByQuery(DeleteUpdateCommand cmd, List<SolrCmdDistributor.Node> replicas, DocCollection coll) throws IOException {
    if (vinfo == null) {
      super.processDelete(cmd);
      return;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    versionDeleteByQuery(cmd);

    doDistribDeleteByQuery(cmd, replicas, coll);


    if (returnVersions && rsp != null) {
      if (deleteByQueryResponse == null) {
        deleteByQueryResponse = new NamedList<>(1);
        rsp.add("deleteByQuery", deleteByQueryResponse);
      }
      deleteByQueryResponse.add(cmd.getQuery(), cmd.getVersion());
    }
  }

  /**
   * This runs after versionDeleteByQuery is invoked, should be used to tamper or forward DeleteCommand
   * @param cmd delete command
   * @param replicas list of Nodes replicas
   * @param coll the collection in zookeeper {@link org.apache.solr.common.cloud.DocCollection}.
   * @throws IOException in case post processing failed
   */
  protected void doDistribDeleteByQuery(DeleteUpdateCommand cmd, List<Node> replicas, DocCollection coll) throws IOException {
    // no-op for derived classes to implement
  }

  protected void versionDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    // Find the version
    long versionOnUpdate = findVersionOnUpdate(cmd);

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;

    if (!leaderLogic && versionOnUpdate == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "missing _version_ on update from leader");
    }

    vinfo.blockUpdates();
    try {

      doLocalDeleteByQuery(cmd, versionOnUpdate, isReplayOrPeersync);

      // since we don't know which documents were deleted, the easiest thing to do is to invalidate
      // all real-time caches (i.e. UpdateLog) which involves also getting a new version of the IndexReader
      // (so cache misses will see up-to-date data)

    } finally {
      vinfo.unblockUpdates();
    }
  }

  private long findVersionOnUpdate(UpdateCommand cmd) {
    long versionOnUpdate = cmd.getVersion();
    if (versionOnUpdate == 0) {
      String versionOnUpdateS = req.getParams().get(CommonParams.VERSION_FIELD);
      versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
    }
    versionOnUpdate = Math.abs(versionOnUpdate);  // normalize to positive version
    return versionOnUpdate;
  }

  private void doLocalDeleteByQuery(DeleteUpdateCommand cmd, long versionOnUpdate, boolean isReplayOrPeersync) throws IOException {
    if (versionsStored) {
      final boolean leaderLogic = isLeader & !isReplayOrPeersync;
      if (leaderLogic) {
        long version = vinfo.getNewClock();
        cmd.setVersion(-version);
        // TODO update versions in all buckets

        doLocalDelete(cmd);

      } else {
        cmd.setVersion(-versionOnUpdate);

        if (ulog.getState() != UpdateLog.State.ACTIVE && isReplayOrPeersync == false) {
          // we're not in an active state, and this update isn't from a replay, so buffer it.
          cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
          ulog.deleteByQuery(cmd);
          return;
        }

        if (!isSubShardLeader && replicaType == Replica.Type.TLOG && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
          // TLOG replica not leader, don't write the DBQ to IW
          cmd.setFlags(cmd.getFlags() | UpdateCommand.IGNORE_INDEXWRITER);
        }
        doLocalDelete(cmd);
      }
    }
  }

  // internal helper method to setup request by processors who use this class.
  // NOTE: not called by this class!
  void setupRequest(UpdateCommand cmd) {
    updateCommand = cmd;
    isLeader = getNonZkLeaderAssumption(req);
  }

  /**
   *
   * @param id id of doc
   * @return url of leader, or null if not found.
   */
  protected String getLeaderUrl(String id) {
    return req.getParams().get(DISTRIB_FROM);
  }

  protected boolean versionDelete(DeleteUpdateCommand cmd) throws IOException {

    BytesRef idBytes = cmd.getIndexedId();

    if (vinfo == null || idBytes == null) {
      super.processDelete(cmd);
      return false;
    }

    // This is only the hash for the bucket, and must be based only on the uniqueKey (i.e. do not use a pluggable hash
    // here)
    int bucketHash = bucketHash(idBytes);

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find the version
    long versionOnUpdate = cmd.getVersion();
    if (versionOnUpdate == 0) {
      String versionOnUpdateS = req.getParams().get(CommonParams.VERSION_FIELD);
      versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
    }
    long signedVersionOnUpdate = versionOnUpdate;
    versionOnUpdate = Math.abs(versionOnUpdate); // normalize to positive version

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;
    boolean forwardedFromCollection = cmd.getReq().getParams().get(DISTRIB_FROM_COLLECTION) != null;

    if (!leaderLogic && versionOnUpdate == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "missing _version_ on update from leader");
    }

    VersionBucket bucket = vinfo.bucket(bucketHash);

    vinfo.lockForUpdate();
    try {
      long finalVersionOnUpdate = versionOnUpdate;
      return bucket.runWithLock(vinfo.getVersionBucketLockTimeoutMs(), () -> doVersionDelete(cmd, finalVersionOnUpdate, signedVersionOnUpdate, isReplayOrPeersync, leaderLogic,
          forwardedFromCollection, bucket));
    } finally {
      vinfo.unlockForUpdate();
    }
  }

  private boolean doVersionDelete(DeleteUpdateCommand cmd, long versionOnUpdate, long signedVersionOnUpdate,
      boolean isReplayOrPeersync, boolean leaderLogic, boolean forwardedFromCollection, VersionBucket bucket)
      throws IOException {
    try {
      BytesRef idBytes = cmd.getIndexedId();
      if (versionsStored) {
        long bucketVersion = bucket.highest;

        if (leaderLogic) {

          if (forwardedFromCollection && ulog.getState() == UpdateLog.State.ACTIVE) {
            // forwarded from a collection but we are not buffering so strip original version and apply our own
            // see SOLR-5308
            if (log.isInfoEnabled()) {
              log.info("Removing version field from doc: {}", cmd.getId());
            }
            versionOnUpdate = signedVersionOnUpdate = 0;
          }

          // leaders can also be in buffering state during "migrate" API call, see SOLR-5308
          if (forwardedFromCollection && ulog.getState() != UpdateLog.State.ACTIVE
              && !isReplayOrPeersync) {
            // we're not in an active state, and this update isn't from a replay, so buffer it.
            if (log.isInfoEnabled()) {
              log.info("Leader logic applied but update log is buffering: {}", cmd.getId());
            }
            cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
            ulog.delete(cmd);
            return true;
          }

          if (signedVersionOnUpdate != 0) {
            Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
            long foundVersion = lastVersion == null ? -1 : lastVersion;
            if ((signedVersionOnUpdate == foundVersion) || (signedVersionOnUpdate < 0 && foundVersion < 0)
                || (signedVersionOnUpdate == 1 && foundVersion > 0)) {
              // we're ok if versions match, or if both are negative (all missing docs are equal), or if cmd
              // specified it must exist (versionOnUpdate==1) and it does.
            } else {
              throw new SolrException(ErrorCode.CONFLICT, "version conflict for " + cmd.getId() + " expected="
                  + signedVersionOnUpdate + " actual=" + foundVersion);
            }
          }

          long version = vinfo.getNewClock();
          cmd.setVersion(-version);
          bucket.updateHighest(version);
        } else {
          cmd.setVersion(-versionOnUpdate);

          if (ulog.getState() != UpdateLog.State.ACTIVE && isReplayOrPeersync == false) {
            // we're not in an active state, and this update isn't from a replay, so buffer it.
            cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
            ulog.delete(cmd);
            return true;
          }

          // if we aren't the leader, then we need to check that updates were not re-ordered
          if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
            // we're OK... this update has a version higher than anything we've seen
            // in this bucket so far, so we know that no reordering has yet occurred.
            bucket.updateHighest(versionOnUpdate);
          } else {
            // there have been updates higher than the current update. we need to check
            // the specific version for this id.
            Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
            if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
              // This update is a repeat, or was reordered. We need to drop this update.
              if (log.isDebugEnabled()) {
                log.debug("Dropping delete update due to version {}", idBytes.utf8ToString());
              }
              return true;
            }
          }

          if (!isSubShardLeader && replicaType == Replica.Type.TLOG && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
            cmd.setFlags(cmd.getFlags() | UpdateCommand.IGNORE_INDEXWRITER);
          }
        }
      }

      doLocalDelete(cmd);
      return false;
    } finally {
      bucket.unlock();
    }
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    
    assert TestInjection.injectFailUpdateRequests();

    updateCommand = cmd;

    // replica type can only be NRT in standalone mode
    // NRT replicas will always commit
    doLocalCommit(cmd);

  }

  protected void doLocalCommit(CommitUpdateCommand cmd) throws IOException {
    if (vinfo != null) {
      long commitVersion = vinfo.getNewClock();
      cmd.setVersion(commitVersion);
      vinfo.lockForUpdate();
    }
    try {

      if (ulog == null || ulog.getState() == UpdateLog.State.ACTIVE || (cmd.getFlags() & UpdateCommand.REPLAY) != 0) {
        super.processCommit(cmd);
      } else {
        if (log.isInfoEnabled()) {
          log.info("Ignoring commit while not ACTIVE - state: {} replay: {}"
              , ulog.getState(), ((cmd.getFlags() & UpdateCommand.REPLAY) != 0));
        }
      }

    } finally {
      if (vinfo != null) {
        vinfo.unlockForUpdate();
      }
    }
  }

  @Override
  public final void finish() throws IOException {
    assert ! finished : "lifecycle sanity check";
    finished = true;

    doDistribFinish();

    super.finish();
  }

  protected void doDistribFinish() throws IOException {
    // no-op for derived classes to implement
  }

  /**
   * Returns a boolean indicating whether or not the caller should behave as
   * if this is the "leader" even when ZooKeeper is not enabled.  
   * (Even in non zk mode, tests may simulate updates to/from a leader)
   */
  public static boolean getNonZkLeaderAssumption(SolrQueryRequest req) {
    DistribPhase phase = 
      DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));

    // if we have been told we are coming from a leader, then we are 
    // definitely not the leader.  Otherwise assume we are.
    return DistribPhase.FROMLEADER != phase;
  }

  public static final class DistributedUpdatesAsyncException extends SolrException {
    public final List<Error> errors;
    public DistributedUpdatesAsyncException(List<Error> errors) {
      super(buildCode(errors), buildMsg(errors), null);
      this.errors = errors;

      // create a merged copy of the metadata from all wrapped exceptions
      NamedList<String> metadata = new NamedList<String>();
      for (Error error : errors) {
        if (error.e instanceof SolrException) {
          SolrException e = (SolrException) error.e;
          NamedList<String> eMeta = e.getMetadata();
          if (null != eMeta) {
            metadata.addAll(eMeta);
          }
        }
      }
      if (0 < metadata.size()) {
        this.setMetadata(metadata);
      }
    }

    /** Helper method for constructor */
    private static int buildCode(List<Error> errors) {
      assert null != errors;
      assert 0 < errors.size();

      int minCode = Integer.MAX_VALUE;
      int maxCode = Integer.MIN_VALUE;
      for (Error error : errors) {
        log.trace("REMOTE ERROR: {}", error);
        minCode = Math.min(error.statusCode, minCode);
        maxCode = Math.max(error.statusCode, maxCode);
      }
      if (minCode == maxCode) {
        // all codes are consistent, use that...
        return minCode;
      } else if (400 <= minCode && maxCode < 500) {
        // all codes are 4xx, use 400
        return ErrorCode.BAD_REQUEST.code;
      } 
      // ...otherwise use sensible default
      return ErrorCode.SERVER_ERROR.code;
    }
    
    /** Helper method for constructor */
    private static String buildMsg(List<Error> errors) {
      assert null != errors;
      assert 0 < errors.size();
      
      if (1 == errors.size()) {
        return "Async exception during distributed update: " + errors.get(0).e.getMessage();
      } else {
        StringBuilder buf = new StringBuilder(errors.size() + " Async exceptions during distributed update: ");
        for (Error error : errors) {
          buf.append("\n");
          buf.append(error.e.getMessage());
        }
        return buf.toString();
      }
    }
  }


  //    Keeps track of the replication factor achieved for a distributed update request
  //    originated in this distributed update processor. A RollupReplicationTracker is the only tracker that will
  //    persist across sub-requests.
  //
  //   Note that the replica that receives the original request has the only RollupReplicationTracker that exists for the
  //   lifetime of the batch. The leader for each shard keeps track of its own achieved replication for its shard
  //   and attaches that to the response to the originating node (i.e. the one with the RollupReplicationTracker).
  //   Followers in general do not need a tracker of any sort with the sole exception of the RollupReplicationTracker
  //   allocated on the original node that receives the top-level request.
  //
  //   DeleteById is tricky. Since the docs are sent one at a time, there has to be some fancy dancing. In the
  //   deleteById case, here are the rules:
  //
  //   If I'm leader, there are two possibilities:
  //     1> I got the original request. This is the hard one. There are two sub-cases:
  //     a> Some document in the request is deleted from the shard I lead. In this case my computed replication
  //        factor counts.
  //     b> No document in the packet is deleted from my shard. In that case I have nothing to say about the
  //        achieved replication factor.
  //
  //     2> I'm a leader and I got the request from some other replica. In this case I can be certain of a couple of things:
  //       a> The document in the request will be deleted from my shard
  //       b> my replication factor counts.
  //
  //   Even the DeleteById case follows the rules for whether a RollupReplicaitonTracker is allocated.
  //   This doesn't matter when it comes to delete-by-query since all leaders get the sub-request.


  public static class RollupRequestReplicationTracker {

    private int achievedRf = Integer.MAX_VALUE;

    public int getAchievedRf() {
      return achievedRf;
    }

    // We want to report only the minimun _ever_ achieved...
    public void testAndSetAchievedRf(int rf) {
      this.achievedRf = Math.min(this.achievedRf, rf);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder("RollupRequestReplicationTracker")
          .append(" achievedRf: ")
          .append(achievedRf);
      return sb.toString();
    }
  }


  // Allocate a LeaderRequestReplicatinTracker if (and only if) we're a leader. If the request comes in to the leader
  // at first, allocate both one of these and a RollupRequestReplicationTracker.
  //
  // Since these are leader-only, all they really have to do is track the individual update request for this shard
  // and return it to be added to the rollup tracker. Which is kind of simple since we get an onSuccess method in
  // SolrCmdDistributor

  public static class LeaderRequestReplicationTracker {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // Since we only allocate one of these on the leader and, by definition, the leader has been found and is running,
    // we have a replication factor of one by default.
    private int achievedRf = 1;

    private final String myShardId;

    public LeaderRequestReplicationTracker(String shardId) {
      this.myShardId = shardId;
    }

    // gives the replication factor that was achieved for this request
    public int getAchievedRf() {
      return achievedRf;
    }

    public void trackRequestResult(Node node, boolean success) {
      if (log.isDebugEnabled()) {
        log.debug("trackRequestResult({}): success? {}, shardId={}", node, success, myShardId);
      }

      if (success) {
        ++achievedRf;
      }
    }

    public String toString() {
      StringBuilder sb = new StringBuilder("LeaderRequestReplicationTracker");
      sb.append(", achievedRf=")
          .append(getAchievedRf())
          .append(" for shard ")
          .append(myShardId);
      return sb.toString();
    }
  }
}
