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
package org.apache.solr.update;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.ID;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase.FROMLEADER;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * This class is useful for performing peer to peer synchronization of recently indexed update commands during
 * recovery process.
 *
 * @lucene.experimental
 */
public class PeerSync implements SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final boolean debug = log.isDebugEnabled();

  private List<String> replicas;
  private int nUpdates;

  private UpdateHandler uhandler;
  private UpdateLog ulog;
  private HttpShardHandlerFactory shardHandlerFactory;
  private ShardHandler shardHandler;
  private List<SyncShardRequest> requests = new ArrayList<>();

  private final boolean cantReachIsSuccess;
  private final boolean doFingerprint;
  private final HttpClient client;
  private final boolean onlyIfActive;
  private SolrCore core;
  private Updater updater;

  private MissedUpdatesFinder missedUpdatesFinder;

  // metrics
  private Timer syncTime;
  private Counter syncErrors;
  private Counter syncSkipped;

  // comparator that sorts by absolute value, putting highest first
  public static Comparator<Long> absComparator = (l1, l2) -> Long.compare(Math.abs(l2), Math.abs(l1));

  private static class SyncShardRequest extends ShardRequest {
    IndexFingerprint fingerprint;
    boolean doFingerprintComparison;
    Exception updateException;
    long totalRequestedUpdates;
  }

  public PeerSync(SolrCore core, List<String> replicas, int nUpdates, boolean cantReachIsSuccess) {
    this(core, replicas, nUpdates, cantReachIsSuccess, false, true);
  }
  
  public PeerSync(SolrCore core, List<String> replicas, int nUpdates, boolean cantReachIsSuccess, boolean onlyIfActive, boolean doFingerprint) {
    this.core = core;
    this.replicas = replicas;
    this.nUpdates = nUpdates;
    this.cantReachIsSuccess = cantReachIsSuccess;
    this.doFingerprint = doFingerprint && !("true".equals(System.getProperty("solr.disableFingerprint")));
    this.client = core.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient();
    this.onlyIfActive = onlyIfActive;
    
    uhandler = core.getUpdateHandler();
    ulog = uhandler.getUpdateLog();
    // TODO: close
    shardHandlerFactory = (HttpShardHandlerFactory) core.getCoreContainer().getShardHandlerFactory();
    shardHandler = shardHandlerFactory.getShardHandler(client);
    this.updater = new Updater(msg(), core);

    core.getCoreMetricManager().registerMetricProducer(SolrInfoBean.Category.REPLICATION.toString(), this);
  }

  public static final String METRIC_SCOPE = "peerSync";

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    syncTime = manager.timer(null, registry, "time", scope, METRIC_SCOPE);
    syncErrors = manager.counter(null, registry, "errors", scope, METRIC_SCOPE);
    syncSkipped = manager.counter(null, registry, "skipped", scope, METRIC_SCOPE);
  }

  public static long percentile(List<Long> arr, float frac) {
    int elem = (int) (arr.size() * frac);
    return Math.abs(arr.get(elem));
  }

  // start of peersync related debug messages.  includes the core name for correlation.
  private String msg() {
    ZkController zkController = uhandler.core.getCoreContainer().getZkController();

    String myURL = "";

    if (zkController != null) {
      myURL = zkController.getBaseUrl();
    }

    // TODO: core name turns up blank in many tests - find URL if cloud enabled?
    return "PeerSync: core="+uhandler.core.getName()+ " url="+myURL +" ";
  }

  /** Returns true if peer sync was successful, meaning that this core may be considered to have the latest updates.
   * It does not mean that the remote replica is in sync with us.
   */
  public PeerSyncResult sync() {
    if (ulog == null) {
      syncErrors.inc();
      return PeerSyncResult.failure();
    }
    Timer.Context timerContext = null;
    try {
      if (log.isInfoEnabled()) {
        log.info("{} START replicas={} nUpdates={}", msg(), replicas, nUpdates);
      }

      // check if we already in sync to begin with 
      if(doFingerprint && alreadyInSync()) {
        syncSkipped.inc();
        return PeerSyncResult.success();
      }

      // measure only when actual sync is performed
      timerContext = syncTime.time();

      // Fire off the requests before getting our own recent updates (for better concurrency)
      // This also allows us to avoid getting updates we don't need... if we got our updates and then got their updates,
      // they would
      // have newer stuff that we also had (assuming updates are going on and are being forwarded).
      for (String replica : replicas) {
        requestVersions(replica);
      }

      long ourLowThreshold, ourHighThreshold;
      List<Long> ourUpdates;
      try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
        ourUpdates = recentUpdates.getVersions(nUpdates);
      }
      
      ourUpdates.sort(absComparator);

      if (ourUpdates.size() > 0) {
        ourLowThreshold = percentile(ourUpdates, 0.8f);
        ourHighThreshold = percentile(ourUpdates, 0.2f);
      } else {
        // we have no versions and hence no frame of reference to tell if we can use a peers
        // updates to bring us into sync
        if (log.isInfoEnabled()) {
          log.info("{} DONE. We have no versions. sync failed.", msg());
        }
        for (;;)  {
          ShardResponse srsp = shardHandler.takeCompletedOrError();
          if (srsp == null) break;
          if (srsp.getException() == null)  {
            @SuppressWarnings({"unchecked"})
            List<Long> otherVersions = (List<Long>)srsp.getSolrResponse().getResponse().get("versions");
            if (otherVersions != null && !otherVersions.isEmpty())  {
              syncErrors.inc();
              return PeerSyncResult.failure(true);
            }
          }
        }
        syncErrors.inc();
        return PeerSyncResult.failure(false);
      }

      this.missedUpdatesFinder = new MissedUpdatesFinder(ourUpdates, msg(), nUpdates, ourLowThreshold, ourHighThreshold);

      for (;;) {
        ShardResponse srsp = shardHandler.takeCompletedOrError();
        if (srsp == null) break;
        boolean success = handleResponse(srsp);
        if (!success) {
          if (log.isInfoEnabled()) {
            log.info("{} DONE. sync failed", msg());
          }
          shardHandler.cancelAll();
          syncErrors.inc();
          return PeerSyncResult.failure();
        }
      }

      // finish up any comparisons with other shards that we deferred
      boolean success = true;
      for (SyncShardRequest sreq : requests) {
        if (sreq.doFingerprintComparison) {
          success = compareFingerprint(sreq);
          if (!success) break;
        }
      }

      if (log.isInfoEnabled()) {
        log.info("{} DONE. sync {}", msg(), (success ? "succeeded" : "failed"));
      }
      if (!success) {
        syncErrors.inc();
      }
      return success ?  PeerSyncResult.success() : PeerSyncResult.failure();
    } finally {
      if (timerContext != null) {
        timerContext.close();
      }
    }
  }

  /**
   * Check if we are already in sync. Simple fingerprint comparison should do
   */
  private boolean alreadyInSync() {
    for (String replica : replicas) {
      requestFingerprint(replica);
    }

    // We only compute fingerprint during leader election. Therefore after heavy indexing,
    // the call to compute fingerprint takes awhile and slows the leader election.
    // So we do it in parallel with fetching the fingerprint from the other replicas
    IndexFingerprint ourFingerprint;
    try {
      ourFingerprint = IndexFingerprint.getFingerprint(core, Long.MAX_VALUE);
    } catch (IOException e) {
      log.warn("Could not confirm if we are already in sync. Continue with PeerSync");
      return false;
    }

    for (;;) {
      ShardResponse srsp = shardHandler.takeCompletedOrError();
      if (srsp == null) break;

      Object replicaFingerprint = null;
      if (srsp.getSolrResponse() != null && srsp.getSolrResponse().getResponse() != null) {
        replicaFingerprint = srsp.getSolrResponse().getResponse().get("fingerprint");
      }

      if (replicaFingerprint == null) {
        log.warn("Replica did not return a fingerprint - possibly an older Solr version or exception");
        continue;
      }

      IndexFingerprint otherFingerprint = IndexFingerprint.fromObject(replicaFingerprint);
      if(IndexFingerprint.compare(otherFingerprint, ourFingerprint) == 0) {
        log.info("We are already in sync. No need to do a PeerSync ");
        return true;
      }
    }

    return false;
  }
  
  
  private void requestFingerprint(String replica) {
    SyncShardRequest sreq = new SyncShardRequest();
    requests.add(sreq);

    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = new ModifiableSolrParams();
    sreq.params = new ModifiableSolrParams();
    sreq.params.set("qt","/get");
    sreq.params.set(DISTRIB,false);
    sreq.params.set("getFingerprint", String.valueOf(Long.MAX_VALUE));
    
    shardHandler.submit(sreq, replica, sreq.params);
  }
  
  
  
  private void requestVersions(String replica) {
    SyncShardRequest sreq = new SyncShardRequest();
    requests.add(sreq);
    sreq.purpose = 1;
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = new ModifiableSolrParams();
    sreq.params.set("qt","/get");
    sreq.params.set(DISTRIB,false);
    sreq.params.set("getVersions",nUpdates);
    sreq.params.set("fingerprint",doFingerprint);
    shardHandler.submit(sreq, replica, sreq.params);
  }

  private boolean handleResponse(ShardResponse srsp) {
    ShardRequest sreq = srsp.getShardRequest();

    if (srsp.getException() != null) {

      // TODO: look at this more thoroughly - we don't want
      // to fail on connection exceptions, but it may make sense
      // to determine this based on the number of fails
      //
      // If the replica went down between asking for versions and asking for specific updates, that
      // shouldn't be treated as success since we counted on getting those updates back (and avoided
      // redundantly asking other replicas for them).
      if (cantReachIsSuccess && sreq.purpose == 1 && srsp.getException() instanceof SolrServerException) {
        Throwable solrException = ((SolrServerException) srsp.getException())
            .getRootCause();
        boolean connectTimeoutExceptionInChain = connectTimeoutExceptionInChain(srsp.getException());
        if (connectTimeoutExceptionInChain || solrException instanceof ConnectTimeoutException || solrException instanceof SocketTimeoutException
            || solrException instanceof NoHttpResponseException || solrException instanceof SocketException) {

          log.warn("{} couldn't connect to {}, counting as success ", msg(), srsp.getShardAddress(), srsp.getException());
          return true;
        }
      }
      
      if (cantReachIsSuccess && sreq.purpose == 1 && srsp.getException() instanceof SolrException && ((SolrException) srsp.getException()).code() == 503) {
        log.warn("{} got a 503 from {}, counting as success ", msg(), srsp.getShardAddress(), srsp.getException());
        return true;
      }
      
      if (cantReachIsSuccess && sreq.purpose == 1 && srsp.getException() instanceof SolrException && ((SolrException) srsp.getException()).code() == 404) {
        log.warn("{} got a 404 from {}, counting as success. {} Perhaps /get is not registered?"
            , msg(), srsp.getShardAddress(), srsp.getException());
        return true;
      }
      
      // TODO: we should return the above information so that when we can request a recovery through zookeeper, we do
      // that for these nodes
      
      // TODO: at least log???
      // srsp.getException().printStackTrace(System.out);

      log.warn("{} exception talking to {}, failed", msg(), srsp.getShardAddress(), srsp.getException());

      return false;
    }

    if (sreq.purpose == 1) {
      return handleVersions(srsp);
    } else {
      return handleUpdates(srsp);
    }
  }
  
  // sometimes the root exception is a SocketTimeoutException, but ConnectTimeoutException
  // is in the chain
  private boolean connectTimeoutExceptionInChain(Throwable exception) {
    Throwable t = exception;
    while (true) {
      if (t instanceof ConnectTimeoutException) {
        return true;
      }
      Throwable cause = t.getCause();
      if (cause != null) {
        t = cause;
      } else {
        return false;
      }
    }
  }

  private boolean handleVersions(ShardResponse srsp) {
    // we retrieved the last N updates from the replica
    @SuppressWarnings({"unchecked"})
    List<Long> otherVersions = (List<Long>)srsp.getSolrResponse().getResponse().get("versions");
    // TODO: how to handle short lists?

    SyncShardRequest sreq = (SyncShardRequest) srsp.getShardRequest();
    Object fingerprint = srsp.getSolrResponse().getResponse().get("fingerprint");

    if (log.isInfoEnabled()) {
      log.info("{} Received {} versions from {} fingerprint:{}", msg(), otherVersions.size(), sreq.shards[0], fingerprint);
    }
    if (fingerprint != null) {
      sreq.fingerprint = IndexFingerprint.fromObject(fingerprint);
    }

    if (otherVersions.size() == 0) {
      // when sync with other replicas, they may not contains any updates
      return true;
    }
    
    MissedUpdatesRequest updatesRequest = missedUpdatesFinder.find(
        otherVersions, sreq.shards[0]);

    if (updatesRequest == MissedUpdatesRequest.ALREADY_IN_SYNC) {
      return true;
    } else if (updatesRequest == MissedUpdatesRequest.UNABLE_TO_SYNC) {
      return false;
    } else if (updatesRequest == MissedUpdatesRequest.EMPTY) {
      // If we requested updates from another replica, we can't compare fingerprints yet with this replica, we need to defer
      if (doFingerprint) {
        sreq.doFingerprintComparison = true;
      }
      return true;
    }

    sreq.totalRequestedUpdates = updatesRequest.totalRequestedUpdates;
    return requestUpdates(srsp, updatesRequest.versionsAndRanges, updatesRequest.totalRequestedUpdates);
  }

  private boolean compareFingerprint(SyncShardRequest sreq) {
    if (sreq.fingerprint == null) return true;
    try {
      // check our fingerprint only upto the max version in the other fingerprint. 
      // Otherwise for missed updates (look at missed update test in PeerSyncTest) ourFingerprint won't match with otherFingerprint   
      IndexFingerprint ourFingerprint = IndexFingerprint.getFingerprint(core, sreq.fingerprint.getMaxVersionSpecified());
      int cmp = IndexFingerprint.compare(sreq.fingerprint, ourFingerprint);
      log.info("Fingerprint comparison: {}" , cmp);
      if(cmp != 0) {
        log.info("Other fingerprint: {}, Our fingerprint: {}", sreq.fingerprint , ourFingerprint);
      }
      return cmp == 0;  // currently, we only check for equality...
    } catch(IOException e){
      log.error("{} Error getting index fingerprint", msg(), e);
      return false;
    }
  }

  private boolean requestUpdates(ShardResponse srsp, String versionsAndRanges, long totalUpdates) {
    String replica = srsp.getShardRequest().shards[0];

    if (log.isInfoEnabled()) {
      log.info("{} Requesting updates from {} n={} versions={}", msg(), replica, totalUpdates, versionsAndRanges);
    }

    // reuse our original request object
    ShardRequest sreq = srsp.getShardRequest();

    sreq.purpose = 0;
    sreq.params = new ModifiableSolrParams();
    sreq.params.set("qt", "/get");
    sreq.params.set(DISTRIB, false);
    sreq.params.set("getUpdates", versionsAndRanges);
    sreq.params.set("onlyIfActive", onlyIfActive);

    sreq.params.set("fingerprint",doFingerprint);
    sreq.responses.clear();  // needs to be zeroed for correct correlation to occur

    shardHandler.submit(sreq, sreq.shards[0], sreq.params);

    return true;
  }


  private boolean handleUpdates(ShardResponse srsp) {
    // we retrieved the last N updates from the replica
    @SuppressWarnings({"unchecked"})
    List<Object> updates = (List<Object>)srsp.getSolrResponse().getResponse().get("updates");

    SyncShardRequest sreq = (SyncShardRequest) srsp.getShardRequest();
    if (updates.size() < sreq.totalRequestedUpdates) {
      log.error("{} Requested {} updates from {} but retrieved {}", msg(), sreq.totalRequestedUpdates, sreq.shards[0], updates.size());
      return false;
    }
    
    // overwrite fingerprint we saved in 'handleVersions()'   
    Object fingerprint = srsp.getSolrResponse().getResponse().get("fingerprint");

    if (fingerprint != null) {
      sreq.fingerprint = IndexFingerprint.fromObject(fingerprint);
    }

    try {
      this.updater.applyUpdates(updates, sreq.shards);
    } catch (Exception e) {
      sreq.updateException = e;
      return false;
    }

    return compareFingerprint(sreq);
  }

  public static class PeerSyncResult  {
    private final boolean success;
    private final Boolean otherHasVersions;

    PeerSyncResult(boolean success, Boolean otherHasVersions) {
      this.success = success;
      this.otherHasVersions = otherHasVersions;
    }

    public boolean isSuccess() {
      return success;
    }

    public Optional<Boolean> getOtherHasVersions() {
      return Optional.ofNullable(otherHasVersions);
    }

    public static PeerSyncResult success()  {
      return new PeerSyncResult(true, null);
    }

    public static PeerSyncResult failure()  {
      return new PeerSyncResult(false, null);
    }

    public static PeerSyncResult failure(boolean otherHasVersions)  {
      return new PeerSyncResult(false, otherHasVersions);
    }
  }

  /**
   * Helper class for apply missed updates
   */
  static class Updater {
    // comparator that sorts update records by absolute value of version, putting lowest first
    private static final Comparator<Object> updateRecordComparator = (o1, o2) -> {
      if (!(o1 instanceof List)) return 1;
      if (!(o2 instanceof List)) return -1;

      @SuppressWarnings({"rawtypes"})
      List lst1 = (List) o1;
      @SuppressWarnings({"rawtypes"})
      List lst2 = (List) o2;

      long l1 = Math.abs((Long) lst1.get(1));
      long l2 = Math.abs((Long) lst2.get(1));

      return Long.compare(l1, l2);
    };

    private String logPrefix;
    private SolrCore solrCore;

    Updater(String logPrefix, SolrCore solrCore) {
      this.logPrefix = logPrefix;
      this.solrCore = solrCore;
    }

    void applyUpdates(List<Object> updates, Object updateFrom) throws Exception {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(DISTRIB_UPDATE_PARAM, FROMLEADER.toString());
      params.set("peersync",true); // debugging
      SolrQueryRequest req = new LocalSolrQueryRequest(solrCore, params);
      SolrQueryResponse rsp = new SolrQueryResponse();

      UpdateRequestProcessorChain processorChain = req.getCore().getUpdateProcessingChain(null);
      UpdateRequestProcessor proc = processorChain.createProcessor(req, rsp);

      updates.sort(updateRecordComparator);

      Object o = null;
      long lastVersion = 0;
      try {
        // Apply oldest updates first
        for (Object obj : updates) {
          // should currently be a List<Oper,Ver,Doc/Id>
          o = obj;
          @SuppressWarnings({"unchecked"})
          List<Object> entry = (List<Object>)o;

          if (debug) {
            log.debug("{} raw update record {}", logPrefix, o);
          }

          int oper = (Integer)entry.get(0) & UpdateLog.OPERATION_MASK;
          long version = (Long) entry.get(1);
          if (version == lastVersion && version != 0) continue;
          lastVersion = version;

          switch (oper) {
            case UpdateLog.ADD:
            {
              // byte[] idBytes = (byte[]) entry.get(2);
              SolrInputDocument sdoc = (SolrInputDocument)entry.get(entry.size()-1);
              AddUpdateCommand cmd = new AddUpdateCommand(req);
              // cmd.setIndexedId(new BytesRef(idBytes));
              cmd.solrDoc = sdoc;
              cmd.setVersion(version);
              cmd.setFlags(UpdateCommand.PEER_SYNC | UpdateCommand.IGNORE_AUTOCOMMIT);
              if (debug) {
                log.debug("{} add {} id {}", logPrefix, cmd, sdoc.getField(ID));
              }
              proc.processAdd(cmd);
              break;
            }
            case UpdateLog.DELETE:
            {
              byte[] idBytes = (byte[]) entry.get(2);
              DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
              cmd.setIndexedId(new BytesRef(idBytes));
              cmd.setVersion(version);
              cmd.setFlags(UpdateCommand.PEER_SYNC | UpdateCommand.IGNORE_AUTOCOMMIT);
              if (debug) {
                if (log.isDebugEnabled()) {
                  log.debug("{} delete {} {}", logPrefix, cmd, new BytesRef(idBytes).utf8ToString());
                }
              }
              proc.processDelete(cmd);
              break;
            }

            case UpdateLog.DELETE_BY_QUERY:
            {
              String query = (String)entry.get(2);
              DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
              cmd.query = query;
              cmd.setVersion(version);
              cmd.setFlags(UpdateCommand.PEER_SYNC | UpdateCommand.IGNORE_AUTOCOMMIT);
              if (debug) {
                log.debug("{} deleteByQuery {}", logPrefix, cmd);
              }
              proc.processDelete(cmd);
              break;
            }
            case UpdateLog.UPDATE_INPLACE:
            {
              AddUpdateCommand cmd = UpdateLog.convertTlogEntryToAddUpdateCommand(req, entry, oper, version);
              cmd.setFlags(UpdateCommand.PEER_SYNC | UpdateCommand.IGNORE_AUTOCOMMIT);
              if (debug) {
                log.debug("{} inplace update {} prevVersion={} doc={}", logPrefix, cmd, cmd.prevVersion, cmd.solrDoc);
              }
              proc.processAdd(cmd);
              break;
            }

            default:
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
          }

        }

      } catch (IOException e) {
        // TODO: should this be handled separately as a problem with us?
        // I guess it probably already will by causing replication to be kicked off.
        log.error("{} Error applying updates from {}, update={}", logPrefix, updateFrom, o, e);
        throw e;
      } catch (Exception e) {
        log.error("{} Error applying updates from {}, update={} ", logPrefix, updateFrom,  o, e);
        throw e;
      } finally {
        try {
          proc.finish();
        } catch (Exception e) {
          log.error("{} Error applying updates from {}, finish()", logPrefix, updateFrom, e);
          throw e;
        } finally {
          IOUtils.closeQuietly(proc);
        }
      }
    }
  }

  static abstract class MissedUpdatesFinderBase {
    long ourLowThreshold;  // 20th percentile
    List<Long> ourUpdates;

    MissedUpdatesFinderBase(List<Long> ourUpdates, long ourLowThreshold) {
      assert sorted(ourUpdates);
      this.ourUpdates = ourUpdates;
      this.ourLowThreshold = ourLowThreshold;
    }

    private boolean sorted(List<Long> list) {
      long prev = Long.MAX_VALUE;
      for (long a : list) {
        if (Math.abs(a) > prev) return false;
        prev = Math.abs(a);
      }
      return true;
    }

    MissedUpdatesRequest handleVersionsWithRanges(List<Long> otherVersions, boolean completeList) {
      // we may endup asking for updates for too many versions, causing 2MB post payload limit. Construct a range of
      // versions to request instead of asking individual versions
      List<String> rangesToRequest = new ArrayList<>();

      // construct ranges to request
      // both ourUpdates and otherVersions are sorted with highest range first
      // may be we can create another reverse the lists and avoid confusion
      int ourUpdatesIndex = ourUpdates.size() - 1;
      int otherUpdatesIndex = otherVersions.size() - 1;
      long totalRequestedVersions = 0;

      while (otherUpdatesIndex >= 0) {
        // we have run out of ourUpdates, pick up all the remaining versions from the other versions
        if (ourUpdatesIndex < 0) {
          String range = otherVersions.get(otherUpdatesIndex) + "..." + otherVersions.get(0);
          rangesToRequest.add(range);
          totalRequestedVersions += otherUpdatesIndex + 1;
          break;
        }

        // stop when the entries get old enough that reorders may lead us to see updates we don't need
        if (!completeList && Math.abs(otherVersions.get(otherUpdatesIndex)) < ourLowThreshold) break;

        if (ourUpdates.get(ourUpdatesIndex).longValue() == otherVersions.get(otherUpdatesIndex).longValue()) {
          ourUpdatesIndex--;
          otherUpdatesIndex--;
        } else if (Math.abs(ourUpdates.get(ourUpdatesIndex)) < Math.abs(otherVersions.get(otherUpdatesIndex))) {
          ourUpdatesIndex--;
        } else {
          long rangeStart = otherVersions.get(otherUpdatesIndex);
          while (otherUpdatesIndex >= 0
              && (Math.abs(otherVersions.get(otherUpdatesIndex)) < Math.abs(ourUpdates.get(ourUpdatesIndex)))) {
            otherUpdatesIndex--;
            totalRequestedVersions++;
          }
          // construct range here
          rangesToRequest.add(rangeStart + "..." + otherVersions.get(otherUpdatesIndex + 1));
        }
      }

      String rangesToRequestStr = rangesToRequest.stream().collect(Collectors.joining(","));
      return MissedUpdatesRequest.of(rangesToRequestStr, totalRequestedVersions);
    }
  }

  /**
   * Helper class for doing comparison ourUpdates and other replicas's updates to find the updates that we missed
   */
  public static class MissedUpdatesFinder extends MissedUpdatesFinderBase {
    private long ourHighThreshold; // 80th percentile
    private long ourHighest;  // currently just used for logging/debugging purposes
    private String logPrefix;
    private long nUpdates;

    MissedUpdatesFinder(List<Long> ourUpdates, String logPrefix, long nUpdates,
                        long ourLowThreshold, long ourHighThreshold) {
      super(ourUpdates, ourLowThreshold);

      this.logPrefix = logPrefix;
      this.ourHighThreshold = ourHighThreshold;
      this.ourHighest = ourUpdates.get(0);
      this.nUpdates = nUpdates;
    }

    public MissedUpdatesRequest find(List<Long> otherVersions, Object updateFrom) {
      otherVersions.sort(absComparator);
      if (debug) {
        log.debug("{} sorted versions from {} = {}", logPrefix, otherVersions, updateFrom);
      }

      long otherHigh = percentile(otherVersions, .2f);
      long otherLow = percentile(otherVersions, .8f);
      long otherHighest = otherVersions.get(0);

      if (ourHighThreshold < otherLow) {
        // Small overlap between version windows and ours is older
        // This means that we might miss updates if we attempted to use this method.
        // Since there exists just one replica that is so much newer, we must
        // fail the sync.
        log.info("{} Our versions are too old. ourHighThreshold={} otherLowThreshold={} ourHighest={} otherHighest={}",
            logPrefix, ourHighThreshold, otherLow, ourHighest, otherHighest);
        return MissedUpdatesRequest.UNABLE_TO_SYNC;
      }

      if (ourLowThreshold > otherHigh && ourHighest >= otherHighest) {
        // Small overlap between windows and ours is newer.
        // Using this list to sync would result in requesting/replaying results we don't need
        // and possibly bringing deleted docs back to life.
        log.info("{} Our versions are newer. ourHighThreshold={} otherLowThreshold={} ourHighest={} otherHighest={}",
            logPrefix, ourHighThreshold, otherLow, ourHighest, otherHighest);

        // Because our versions are newer, IndexFingerprint with the remote would not match us.
        // We return true on our side, but the remote peersync with us should fail.
        return MissedUpdatesRequest.ALREADY_IN_SYNC;
      }

      boolean completeList = otherVersions.size() < nUpdates;

      MissedUpdatesRequest updatesRequest = handleVersionsWithRanges(otherVersions, completeList);
      if (updatesRequest.totalRequestedUpdates > nUpdates) {
        log.info("{} PeerSync will fail because number of missed updates is more than:{}", logPrefix, nUpdates);
        return MissedUpdatesRequest.UNABLE_TO_SYNC;
      }

      if (updatesRequest == MissedUpdatesRequest.EMPTY) {
        log.info("{} No additional versions requested. ourHighThreshold={} otherLowThreshold={} ourHighest={} otherHighest={}",
            logPrefix, ourHighThreshold, otherLow, ourHighest, otherHighest);
      }

      return updatesRequest;
    }
  }

  /**
   * Result of {@link MissedUpdatesFinder}
   */
  public static class MissedUpdatesRequest {
    static final MissedUpdatesRequest UNABLE_TO_SYNC = new MissedUpdatesRequest();
    static final MissedUpdatesRequest ALREADY_IN_SYNC = new MissedUpdatesRequest();
    public static final MissedUpdatesRequest EMPTY = new MissedUpdatesRequest();

    String versionsAndRanges;
    long totalRequestedUpdates;

    private MissedUpdatesRequest(){}

    public static MissedUpdatesRequest of(String versionsAndRanges, long totalRequestedUpdates) {
      if (totalRequestedUpdates == 0) return EMPTY;
      return new MissedUpdatesRequest(versionsAndRanges, totalRequestedUpdates);
    }

    MissedUpdatesRequest(String versionsAndRanges, long totalRequestedUpdates) {
      this.versionsAndRanges = versionsAndRanges;
      this.totalRequestedUpdates = totalRequestedUpdates;
    }
  }
  
}
