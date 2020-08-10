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
import java.util.List;
import java.util.Set;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.update.PeerSync.MissedUpdatesRequest;
import static org.apache.solr.update.PeerSync.absComparator;
import static org.apache.solr.update.PeerSync.percentile;

public class PeerSyncWithLeader implements SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean debug = log.isDebugEnabled();

  private String leaderUrl;
  private int nUpdates;

  private UpdateHandler uhandler;
  private UpdateLog ulog;
  private HttpSolrClient clientToLeader;

  private boolean doFingerprint;

  private SolrCore core;
  private PeerSync.Updater updater;
  private MissedUpdatesFinder missedUpdatesFinder;
  private Set<Long> bufferedUpdates;

  // metrics
  private Timer syncTime;
  private Counter syncErrors;
  private Counter syncSkipped;

  public PeerSyncWithLeader(SolrCore core, String leaderUrl, int nUpdates) {
    this.core = core;
    this.leaderUrl = leaderUrl;
    this.nUpdates = nUpdates;

    this.doFingerprint = !"true".equals(System.getProperty("solr.disableFingerprint"));
    this.uhandler = core.getUpdateHandler();
    this.ulog = uhandler.getUpdateLog();
    HttpClient httpClient = core.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient();
    this.clientToLeader = new HttpSolrClient.Builder(leaderUrl).withHttpClient(httpClient).build();

    this.updater = new PeerSync.Updater(msg(), core);

    core.getCoreMetricManager().registerMetricProducer(SolrInfoBean.Category.REPLICATION.toString(), this);
  }

  public static final String METRIC_SCOPE = "peerSync";

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    syncTime = manager.timer(null, registry, "time", scope, METRIC_SCOPE);
    syncErrors = manager.counter(null, registry, "errors", scope, METRIC_SCOPE);
    syncSkipped = manager.counter(null, registry, "skipped", scope, METRIC_SCOPE);
  }

  // start of peersync related debug messages.  includes the core name for correlation.
  private String msg() {
    ZkController zkController = uhandler.core.getCoreContainer().getZkController();
    String myURL = "";
    if (zkController != null) {
      myURL = zkController.getBaseUrl();
    }

    return "PeerSync: core="+uhandler.core.getName()+ " url="+myURL +" ";
  }

  /**
   * Sync with leader
   * @param startingVersions : recent versions on startup
   * @return result of PeerSync with leader
   */
  public PeerSync.PeerSyncResult sync(List<Long> startingVersions){
    if (ulog == null) {
      syncErrors.inc();
      return PeerSync.PeerSyncResult.failure();
    }

    if (startingVersions.isEmpty()) {
      log.warn("no frame of reference to tell if we've missed updates");
      syncErrors.inc();
      return PeerSync.PeerSyncResult.failure();
    }

    Timer.Context timerContext = null;
    try {
      if (log.isInfoEnabled()) {
        log.info("{} START leader={} nUpdates={}", msg(), leaderUrl, nUpdates);
      }

      if (debug) {
        log.debug("{} startingVersions={} {}", msg(), startingVersions.size(), startingVersions);
      }
      // check if we already in sync to begin with
      if(doFingerprint && alreadyInSync()) {
        syncSkipped.inc();
        return PeerSync.PeerSyncResult.success();
      }

      // measure only when actual sync is performed
      timerContext = syncTime.time();

      List<Long> ourUpdates;
      try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
        ourUpdates = recentUpdates.getVersions(nUpdates);
        bufferedUpdates = recentUpdates.getBufferUpdates();
      }

      ourUpdates.sort(absComparator);
      startingVersions.sort(absComparator);

      long ourLowThreshold = percentile(startingVersions, 0.8f);
      long ourHighThreshold = percentile(startingVersions, 0.2f);

      // now make sure that the starting updates overlap our updates
      // there shouldn't be reorders, so any overlap will do.
      long smallestNewUpdate = Math.abs(ourUpdates.get(ourUpdates.size() - 1));

      if (Math.abs(startingVersions.get(0)) < smallestNewUpdate) {
        log.warn("{} too many updates received since start - startingUpdates no longer overlaps with our currentUpdates", msg());
        syncErrors.inc();
        return PeerSync.PeerSyncResult.failure();
      }

      // let's merge the lists
      for (Long ver : startingVersions) {
        if (Math.abs(ver) < smallestNewUpdate) {
          ourUpdates.add(ver);
        }
      }

      boolean success = doSync(ourUpdates, ourLowThreshold, ourHighThreshold);

      if (log.isInfoEnabled()) {
        log.info("{} DONE. sync {}", msg(), (success ? "succeeded" : "failed"));
      }
      if (!success) {
        syncErrors.inc();
      }
      return success ?  PeerSync.PeerSyncResult.success() : PeerSync.PeerSyncResult.failure();
    } finally {
      if (timerContext != null) {
        timerContext.close();
      }
      try {
        clientToLeader.close();
      } catch (IOException e) {
        log.warn("{} unable to close client to leader", msg(), e);
      }
    }
  }

  private boolean doSync(List<Long> ourUpdates, long ourLowThreshold, long ourHighThreshold) {
    // get leader's recent versions and fingerprint
    // note: by getting leader's versions later, we guarantee that leader's versions always super set of {@link bufferedUpdates}
    NamedList<Object> leaderVersionsAndFingerprint = getVersions();
    IndexFingerprint leaderFingerprint = getFingerprint(leaderVersionsAndFingerprint);
    if (doFingerprint) {
      if (leaderFingerprint == null) {
        log.warn("Could not get fingerprint from the leader");
        return false;
      }
      log.info("Leader fingerprint {}", leaderFingerprint);
    }

    missedUpdatesFinder = new MissedUpdatesFinder(ourUpdates, msg(), nUpdates, ourLowThreshold);
    MissedUpdatesRequest missedUpdates = buildMissedUpdatesRequest(leaderVersionsAndFingerprint);
    if (missedUpdates == MissedUpdatesRequest.ALREADY_IN_SYNC) return true;
    if (missedUpdates != MissedUpdatesRequest.UNABLE_TO_SYNC) {
      NamedList<Object> missedUpdatesRsp = requestUpdates(missedUpdates);
      if (handleUpdates(missedUpdatesRsp, missedUpdates.totalRequestedUpdates, leaderFingerprint)) {
        if (doFingerprint) {
          return compareFingerprint(leaderFingerprint);
        }
        return true;
      }
    }
    return false;
  }

  private MissedUpdatesRequest buildMissedUpdatesRequest(NamedList<Object> rsp) {
    // we retrieved the last N updates from the replica
    @SuppressWarnings({"unchecked"})
    List<Long> otherVersions = (List<Long>)rsp.get("versions");
    if (log.isInfoEnabled()) {
      log.info("{} Received {} versions from {}", msg(), otherVersions.size(), leaderUrl);
    }

    if (otherVersions.isEmpty()) {
      return MissedUpdatesRequest.UNABLE_TO_SYNC;
    }

    MissedUpdatesRequest updatesRequest = missedUpdatesFinder.find(otherVersions, leaderUrl);
    if (updatesRequest == MissedUpdatesRequest.EMPTY) {
      if (doFingerprint) return MissedUpdatesRequest.UNABLE_TO_SYNC;
      return MissedUpdatesRequest.ALREADY_IN_SYNC;
    }

    return updatesRequest;
  }

  private NamedList<Object> requestUpdates(MissedUpdatesRequest missedUpdatesRequest) {
    if (log.isInfoEnabled()) {
      log.info("{} Requesting updates from {} n={} versions={}", msg(), leaderUrl
          , missedUpdatesRequest.totalRequestedUpdates, missedUpdatesRequest.versionsAndRanges);
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/get");
    params.set(DISTRIB, false);
    params.set("getUpdates", missedUpdatesRequest.versionsAndRanges);
    params.set("onlyIfActive", false);
    params.set("skipDbq", true);

    return request(params, "Failed on getting missed updates from the leader");
  }

  private boolean handleUpdates(NamedList<Object> rsp, long numRequestedUpdates, IndexFingerprint leaderFingerprint) {
    // missed updates from leader, it does not contains updates from bufferedUpdates
    @SuppressWarnings({"unchecked"})
    List<Object> updates = (List<Object>)rsp.get("updates");

    if (updates.size() < numRequestedUpdates) {
      log.error("{} Requested {} updated from {} but retrieved {}", msg(), numRequestedUpdates, leaderUrl, updates.size());
      return false;
    }

    // by apply buffering update, replica will have fingerprint equals to leader.
    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      for (Long bufferUpdate : bufferedUpdates) {
        // updater will sort updates before apply
        updates.add(recentUpdates.lookup(bufferUpdate));
      }
    }

    // Leader will compute its fingerprint, then retrieve its recent updates versions.
    // There are a case that some updates (gap) get into recent versions but do not exist in index (fingerprint).
    // If the gap do not contains DBQ or DBI, it is safe to use leaderFingerprint.maxVersionEncountered as a cut point.
    // TODO leader should do fingerprint and retrieve recent updates version in atomic
    if (leaderFingerprint != null) {
      boolean existDBIOrDBQInTheGap = updates.stream().anyMatch(e -> {
        @SuppressWarnings({"unchecked"})
        List<Object> u = (List<Object>) e;
        long version = (Long) u.get(1);
        int oper = (Integer)u.get(0) & UpdateLog.OPERATION_MASK;
        // only DBI or DBQ in the gap (above) will satisfy this predicate
        return version > leaderFingerprint.getMaxVersionEncountered() && (oper == UpdateLog.DELETE || oper == UpdateLog.DELETE_BY_QUERY);
      });
      if (!existDBIOrDBQInTheGap) {
        // it is safe to use leaderFingerprint.maxVersionEncountered as cut point now.
        updates.removeIf(e -> {
          @SuppressWarnings({"unchecked"})
          List<Object> u = (List<Object>) e;
          long version = (Long) u.get(1);
          return version > leaderFingerprint.getMaxVersionEncountered();
        });
      }
    }

    try {
      updater.applyUpdates(updates, leaderUrl);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private NamedList<Object> request(ModifiableSolrParams params, String onFail) {
    try {
      QueryResponse rsp = new QueryRequest(params, SolrRequest.METHOD.POST).process(clientToLeader);
      Exception exception = rsp.getException();
      if (exception != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, onFail);
      }
      return rsp.getResponse();
    } catch (SolrServerException | IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, onFail);
    }
  }

  private NamedList<Object> getVersions() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt","/get");
    params.set(DISTRIB,false);
    params.set("getVersions",nUpdates);
    params.set("fingerprint",doFingerprint);

    return request(params, "Failed to get recent versions from leader");
  }

  private boolean alreadyInSync() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/get");
    params.set(DISTRIB,false);
    params.set("getFingerprint", String.valueOf(Long.MAX_VALUE));

    NamedList<Object> rsp = request(params, "Failed to get fingerprint from leader");
    IndexFingerprint leaderFingerprint = getFingerprint(rsp);
    return compareFingerprint(leaderFingerprint);
  }

  private IndexFingerprint getFingerprint(NamedList<Object> rsp) {
    Object fingerprint = null;
    if (rsp != null) fingerprint = rsp.get("fingerprint");
    if (fingerprint == null) return null;
    return IndexFingerprint.fromObject(fingerprint);
  }

  private boolean compareFingerprint(IndexFingerprint leaderFingerprint) {
    if (leaderFingerprint == null) {
      log.warn("Replica did not return a fingerprint - possibly an older Solr version or exception");
      return false;
    }

    try {
      IndexFingerprint ourFingerprint = IndexFingerprint.getFingerprint(core, Long.MAX_VALUE);
      int cmp = IndexFingerprint.compare(leaderFingerprint, ourFingerprint);
      log.info("Fingerprint comparison result: {}" , cmp);
      if (cmp != 0) {
        log.info("Leader fingerprint: {}, Our fingerprint: {}", leaderFingerprint , ourFingerprint);
      }
      return cmp == 0;  // currently, we only check for equality...
    } catch (IOException e) {
      log.warn("Could not confirm if we are already in sync. Continue with PeerSync");
    }
    return false;
  }

  /**
   * Helper class for doing comparison ourUpdates and other replicas's updates to find the updates that we missed
   */
  public static class MissedUpdatesFinder extends PeerSync.MissedUpdatesFinderBase {
    private long ourHighest;
    private String logPrefix;
    private long nUpdates;

    MissedUpdatesFinder(List<Long> ourUpdates, String logPrefix, long nUpdates,
                        long ourLowThreshold) {
      super(ourUpdates, ourLowThreshold);

      this.logPrefix = logPrefix;
      this.ourHighest = ourUpdates.get(0);
      this.nUpdates = nUpdates;
    }

    public MissedUpdatesRequest find(List<Long> leaderVersions, Object updateFrom) {
      leaderVersions.sort(absComparator);
      log.debug("{} sorted versions from {} = {}", logPrefix, leaderVersions, updateFrom);

      long leaderLowest = leaderVersions.get(leaderVersions.size() - 1);
      if (Math.abs(ourHighest) < Math.abs(leaderLowest)) {
        log.info("{} Our versions are too old comparing to leader, ourHighest={} otherLowest={}", logPrefix, ourHighest, leaderLowest);
        return MissedUpdatesRequest.UNABLE_TO_SYNC;
      }
      // we don't have to check the case we ahead of the leader.
      // (maybe we are the old leader and we contain some updates that no one have)
      // In that case, we will fail on compute fingerprint with the current leader and start segments replication

      boolean completeList = leaderVersions.size() < nUpdates;
      MissedUpdatesRequest updatesRequest = handleVersionsWithRanges(leaderVersions, completeList);

      if (updatesRequest.totalRequestedUpdates > nUpdates) {
        log.info("{} PeerSync will fail because number of missed updates is more than:{}", logPrefix, nUpdates);
        return MissedUpdatesRequest.UNABLE_TO_SYNC;
      }

      if (updatesRequest == MissedUpdatesRequest.EMPTY) {
        log.info("{} No additional versions requested", logPrefix);
      }

      return updatesRequest;
    }
  }

}
