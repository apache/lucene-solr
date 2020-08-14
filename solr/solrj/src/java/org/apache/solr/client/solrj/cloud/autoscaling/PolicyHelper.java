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

package org.apache.solr.client.solrj.cloud.autoscaling;


import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester.Hint;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.ConditionalMapWriter;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.solr.client.solrj.cloud.autoscaling.Suggestion.Type.improvement;
import static org.apache.solr.client.solrj.cloud.autoscaling.Suggestion.Type.repair;
import static org.apache.solr.client.solrj.cloud.autoscaling.Suggestion.Type.unresolved_violation;
import static org.apache.solr.client.solrj.cloud.autoscaling.Suggestion.Type.violation;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.FREEDISK;
import static org.apache.solr.common.ConditionalMapWriter.dedupeKeyPredicate;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;
import static org.apache.solr.common.params.CoreAdminParams.NODE;
import static org.apache.solr.common.util.Utils.handleExp;
import static org.apache.solr.common.util.Utils.time;
import static org.apache.solr.common.util.Utils.timeElapsed;

public class PolicyHelper {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String POLICY_MAPPING_KEY = "PolicyHelper.policyMapping";

  private static ThreadLocal<Map<String, String>> getPolicyMapping(SolrCloudManager cloudManager) {
    return (ThreadLocal<Map<String, String>>) cloudManager.getObjectCache()
        .computeIfAbsent(POLICY_MAPPING_KEY, k -> new ThreadLocal<>());
  }

  public static List<ReplicaPosition> getReplicaLocations(String collName, AutoScalingConfig autoScalingConfig,
                                                          SolrCloudManager cloudManager,
                                                          Map<String, String> optionalPolicyMapping,
                                                          List<String> shardNames,
                                                          int nrtReplicas,
                                                          int tlogReplicas,
                                                          int pullReplicas,
                                                          List<String> nodesList) {
    List<ReplicaPosition> positions = new ArrayList<>();
    ThreadLocal<Map<String, String>> policyMapping = getPolicyMapping(cloudManager);
    ClusterStateProvider stateProvider = new DelegatingClusterStateProvider(cloudManager.getClusterStateProvider()) {
      @Override
      public String getPolicyNameByCollection(String coll) {
        return policyMapping.get() != null && policyMapping.get().containsKey(coll) ?
            optionalPolicyMapping.get(coll) :
            delegate.getPolicyNameByCollection(coll);
      }
    };
    SolrCloudManager delegatingManager = new DelegatingCloudManager(cloudManager) {
      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return stateProvider;
      }

      @Override
      public DistribStateManager getDistribStateManager() {
        if (autoScalingConfig != null) {
          return new DelegatingDistribStateManager(null) {
            @Override
            public AutoScalingConfig getAutoScalingConfig() {
              return autoScalingConfig;
            }
          };
        } else {
          return super.getDistribStateManager();
        }
      }
    };

    policyMapping.set(optionalPolicyMapping);
    SessionWrapper sessionWrapper = null;
    Policy.Session session = null;
    try {
      try {
        SESSION_WRAPPPER_REF.set(sessionWrapper = getSession(delegatingManager));
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "unable to get autoscaling policy session", e);

      }
      session = sessionWrapper.session;
      Map<String, Double> diskSpaceReqd = new HashMap<>();
      try {
        DocCollection coll = cloudManager.getClusterStateProvider().getCollection(collName);
        if (coll != null) {
          for (String shardName : shardNames) {
            Replica ldr = coll.getLeader(shardName);
            if (ldr != null && cloudManager.getClusterStateProvider().getLiveNodes().contains(ldr.getNodeName())) {
              Map<String, Map<String, List<ReplicaInfo>>> details = cloudManager.getNodeStateProvider().getReplicaInfo(ldr.getNodeName(),
                  Collections.singleton(FREEDISK.perReplicaValue));
              ReplicaInfo replicaInfo = details.getOrDefault(collName, emptyMap()).getOrDefault(shardName, singletonList(null)).get(0);
              if (replicaInfo != null) {
                Object idxSz = replicaInfo.getVariables().get(FREEDISK.perReplicaValue);
                if (idxSz != null) {
                  diskSpaceReqd.put(shardName, 1.5 * (Double) Variable.Type.FREEDISK.validate(null, idxSz, false));
                }
              }
            }

          }
        }
      } catch (IOException e) {
        log.warn("Exception while reading disk free metric values for nodes to be used for collection: " + collName, e);
      }


      Map<Replica.Type, Integer> typeVsCount = new EnumMap<>(Replica.Type.class);
      typeVsCount.put(Replica.Type.NRT, nrtReplicas);
      typeVsCount.put(Replica.Type.TLOG, tlogReplicas);
      typeVsCount.put(Replica.Type.PULL, pullReplicas);
      for (String shardName : shardNames) {
        int idx = 0;
        for (Map.Entry<Replica.Type, Integer> e : typeVsCount.entrySet()) {
          for (int i = 0; i < e.getValue(); i++) {
            Suggester suggester = session.getSuggester(ADDREPLICA)
                .hint(Hint.REPLICATYPE, e.getKey())
                .hint(Hint.COLL_SHARD, new Pair<>(collName, shardName));
            if (nodesList != null) {
              for (String nodeName : nodesList) {
                suggester = suggester.hint(Hint.TARGET_NODE, nodeName);
              }
            }
            if (diskSpaceReqd.get(shardName) != null) {
              suggester.hint(Hint.MINFREEDISK, diskSpaceReqd.get(shardName));
            }
            SolrRequest op = suggester.getSuggestion();
            if (op == null) {
              String errorId = "AutoScaling.error.diagnostics." + System.nanoTime();
              Policy.Session sessionCopy = suggester.session;
              log.error("errorId : " + errorId + "  " +
                  handleExp(log, "", () -> Utils.writeJson(getDiagnostics(sessionCopy), new StringWriter(), true).toString()));

              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, " No node can satisfy the rules " +
                  Utils.toJSONString(Utils.getDeepCopy(session.expandedClauses, 4, true) + " More details from logs in node : "
                      + Utils.getMDCNode() + ", errorId : " + errorId));
            }
            session = suggester.getSession();
            positions.add(new ReplicaPosition(shardName, ++idx, e.getKey(), op.getParams().get(NODE)));
          }
        }
      }
    } finally {
      policyMapping.remove();
      if (sessionWrapper != null) {
        sessionWrapper.returnSession(session);
      }
    }
    return positions;
  }


  public static final int SESSION_EXPIRY = 180; // 3 minutes

  public static MapWriter getDiagnostics(Policy policy, SolrCloudManager cloudManager) {
    Policy.Session session = policy.createSession(cloudManager);
    return getDiagnostics(session);
  }

  public static MapWriter getDiagnostics(Policy.Session session) {
    List<Row> sorted = session.getSortedNodes();
    return ew -> {
      writeNodes(ew, sorted);
      ew.put("liveNodes", session.cloudManager.getClusterStateProvider().getLiveNodes())
          .put("violations", session.getViolations())
          .put("config", session.getPolicy());
    };
  }

  static void writeNodes(MapWriter.EntryWriter ew, List<Row> sorted) throws IOException {
    Set<CharSequence> alreadyWritten = new HashSet<>();
    BiPredicate<CharSequence, Object> p = dedupeKeyPredicate(alreadyWritten)
        .and(ConditionalMapWriter.NON_NULL_VAL)
        .and((s, o) -> !(o instanceof Map) || !((Map) o).isEmpty());
    ew.put("sortedNodes", (IteratorWriter) iw -> {
      for (Row row : sorted) {
        iw.add((MapWriter) ew1 -> {
          alreadyWritten.clear();
          ew1.put("node", row.node, p).
              put("isLive", row.isLive, p);
          for (Cell cell : row.getCells())
            ew1.put(cell.name, cell.val, p);
          ew1.put("replicas", row.collectionVsShardVsReplicas);
        });
      }
    });
  }
  public static List<Suggester.SuggestionInfo> getSuggestions(AutoScalingConfig autoScalingConf,
                                                              SolrCloudManager cloudManager, SolrParams params) {
    return getSuggestions(autoScalingConf, cloudManager, 20, 10, params);
  }

  public static List<Suggester.SuggestionInfo> getSuggestions(AutoScalingConfig autoScalingConf,
                                                              SolrCloudManager cloudManager) {
    return getSuggestions(autoScalingConf, cloudManager, 20, 10, null);
  }


  public static List<Suggester.SuggestionInfo> getSuggestions(AutoScalingConfig autoScalingConf,
                                                              SolrCloudManager cloudManager, int max, int timeoutInSecs, SolrParams params) {
    Policy policy = autoScalingConf.getPolicy();
    Suggestion.Ctx ctx = new Suggestion.Ctx();
    ctx.endTime = cloudManager.getTimeSource().getTimeNs() + TimeUnit.SECONDS.toNanos(timeoutInSecs);
    ctx.max = max;
    ctx.session = policy.createSession(cloudManager);
    String[] t = params == null ? null : params.getParams("type");
    List<String> types = t == null? Collections.EMPTY_LIST: Arrays.asList(t);

    if(types.isEmpty() || types.contains(violation.name())) {
      List<Violation> violations = ctx.session.getViolations();
      for (Violation violation : violations) {
        violation.getClause().getThirdTag().varType.getSuggestions(ctx.setViolation(violation));
        ctx.violation = null;
      }

      for (Violation current : ctx.session.getViolations()) {
        for (Violation old : violations) {
          if (!ctx.needMore()) return ctx.getSuggestions();
          if (current.equals(old)) {
            //could not be resolved
            ctx.suggestions.add(new Suggester.SuggestionInfo(current, null, unresolved_violation));
            break;
          }
        }
      }
    }

    if(types.isEmpty() || types.contains(repair.name())) {
      if (ctx.needMore()) {
        try {
          addMissingReplicas(cloudManager, ctx);
        } catch (IOException e) {
          log.error("Unable to fetch cluster state", e);
        }
      }
    }

    if(types.isEmpty() || types.contains(improvement.name())) {
      if (ctx.needMore()) {
        suggestOptimizations(ctx, Math.min(ctx.max - ctx.getSuggestions().size(), 10));
      }
    }
    return ctx.getSuggestions();
  }

  private static void addMissingReplicas(SolrCloudManager cloudManager, Suggestion.Ctx ctx) throws IOException {
    cloudManager.getClusterStateProvider().getClusterState().forEachCollection(coll -> coll.forEach(slice -> {
      if (!ctx.needMore()) return;
          ReplicaCount replicaCount = new ReplicaCount();
          slice.forEach(replica -> {
            if (replica.getState() == Replica.State.ACTIVE || replica.getState() == Replica.State.RECOVERING) {
              replicaCount.increment(replica.getType());
            }
          });
          addMissingReplicas(replicaCount, coll, slice.getName(), Replica.Type.NRT, ctx);
          addMissingReplicas(replicaCount, coll, slice.getName(), Replica.Type.PULL, ctx);
          addMissingReplicas(replicaCount, coll, slice.getName(), Replica.Type.TLOG, ctx);
        }
    ));
  }

  private static void addMissingReplicas(ReplicaCount count, DocCollection coll, String shard, Replica.Type type, Suggestion.Ctx ctx) {
    int delta = count.delta(coll.getExpectedReplicaCount(type, 0), type);
    for (; ; ) {
      if (!ctx.needMore()) return;
      if (delta >= 0) break;
      SolrRequest suggestion = ctx.addSuggestion(
          ctx.session.getSuggester(ADDREPLICA)
              .hint(Hint.REPLICATYPE, type)
              .hint(Hint.COLL_SHARD, new Pair(coll.getName(), shard)), Suggestion.Type.repair);
      if (suggestion == null) return;
      delta++;
    }
  }


  private static void suggestOptimizations(Suggestion.Ctx ctx, int count) {
    int maxTotalSuggestions = ctx.getSuggestions().size() + count;
    List<Row> matrix = ctx.session.matrix;
    if (matrix.isEmpty()) return;
    for (int i = 0; i < matrix.size(); i++) {
      if (ctx.getSuggestions().size() >= maxTotalSuggestions || ctx.hasTimedOut()) break;
      Row row = matrix.get(i);
      Map<String, Collection<String>> collVsShards = new HashMap<>();
      row.forEachReplica(ri -> collVsShards.computeIfAbsent(ri.getCollection(), s -> new HashSet<>()).add(ri.getShard()));
      for (Map.Entry<String, Collection<String>> e : collVsShards.entrySet()) {
        e.setValue(FreeDiskVariable.getSortedShards(Collections.singletonList(row), e.getValue(), e.getKey()));
      }
      for (Map.Entry<String, Collection<String>> e : collVsShards.entrySet()) {
        if (!ctx.needMore()) return;
        if (ctx.getSuggestions().size() >= maxTotalSuggestions || ctx.hasTimedOut()) break;
        for (String shard : e.getValue()) {
          Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
              .hint(Hint.COLL_SHARD, new Pair<>(e.getKey(), shard))
              .hint(Hint.SRC_NODE, row.node);
          ctx.addSuggestion(suggester, Suggestion.Type.improvement);
          if (ctx.getSuggestions().size() >= maxTotalSuggestions) break;
        }
      }
    }
  }


  /**
   * Use this to dump the state of a system and to generate a testcase
   */
  public static void logState(SolrCloudManager cloudManager, Suggester suggester) {
    if (log.isTraceEnabled()) {
      try {
        log.trace("LOGSTATE: {}",
            Utils.writeJson(loggingInfo(cloudManager.getDistribStateManager().getAutoScalingConfig().getPolicy(), cloudManager, suggester),
                new StringWriter(), true).toString());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }


  static MapWriter loggingInfo(Policy policy, SolrCloudManager cloudManager, Suggester suggester) {
    return ew -> {
      ew.put("diagnostics", getDiagnostics(policy,
          cloudManager));
      if (suggester != null) {
        ew.put("suggester", suggester);
      }
    };
  }

  public enum Status {
    NULL,
    //it is just created and not yet used or all operations on it has been completed fully
    UNUSED,
    COMPUTING, EXECUTING
  }

  /**
   * This class stores a session for sharing purpose. If a process creates a session to
   * compute operations,
   * 1) see if there is a session that is available in the cache,
   * 2) if yes, check if it is expired
   * 3) if it is expired, create a new session
   * 4) if it is not expired, borrow it
   * 5) after computing operations put it back in the cache
   */
  static class SessionRef {
    private final Object lockObj = new Object();
    private SessionWrapper sessionWrapper = SessionWrapper.DEFAULT_INSTANCE;


    public SessionRef() {
    }


    //only for debugging
    SessionWrapper getSessionWrapper() {
      return sessionWrapper;
    }

    /**
     * All operations suggested by the current session object
     * is complete. Do not even cache anything
     */
    private void release(SessionWrapper sessionWrapper) {
      synchronized (lockObj) {
        if (sessionWrapper.createTime == this.sessionWrapper.createTime && this.sessionWrapper.refCount.get() <= 0) {
          log.debug("session set to NULL");
          this.sessionWrapper = SessionWrapper.DEFAULT_INSTANCE;
        } // else somebody created a new session b/c of expiry . So no need to do anything about it
      }
    }

    /**
     * Computing is over for this session and it may contain a new session with new state
     * The session can be used by others while the caller is performing operations
     */
    private void returnSession(SessionWrapper sessionWrapper) {
      TimeSource timeSource = sessionWrapper.session != null ? sessionWrapper.session.cloudManager.getTimeSource() : TimeSource.NANO_TIME;
      synchronized (lockObj) {
        sessionWrapper.status = Status.EXECUTING;
        log.debug("returnSession, curr-time {} sessionWrapper.createTime {}, this.sessionWrapper.createTime {} ", time(timeSource, MILLISECONDS),
            sessionWrapper.createTime,
            this.sessionWrapper.createTime);
        if (sessionWrapper.createTime == this.sessionWrapper.createTime) {
          //this session was used for computing new operations and this can now be used for other
          // computing
          this.sessionWrapper = sessionWrapper;

          //one thread who is waiting for this need to be notified.
          lockObj.notify();
        } else {
          log.debug("create time NOT SAME {} ", SessionWrapper.DEFAULT_INSTANCE.createTime);
          //else just ignore it
        }
      }

    }


    public SessionWrapper get(SolrCloudManager cloudManager) throws IOException, InterruptedException {
      TimeSource timeSource = cloudManager.getTimeSource();
      synchronized (lockObj) {
        if (sessionWrapper.status == Status.NULL ||
            sessionWrapper.zkVersion != cloudManager.getDistribStateManager().getAutoScalingConfig().getZkVersion() ||
            TimeUnit.SECONDS.convert(timeSource.getTimeNs() - sessionWrapper.lastUpdateTime, TimeUnit.NANOSECONDS) > SESSION_EXPIRY) {
          //no session available or the session is expired
          return createSession(cloudManager);
        } else {
          long waitStart = time(timeSource, MILLISECONDS);
          //the session is not expired
          log.debug("reusing a session {}", this.sessionWrapper.createTime);
          if (this.sessionWrapper.status == Status.UNUSED || this.sessionWrapper.status == Status.EXECUTING) {
            this.sessionWrapper.status = Status.COMPUTING;
            return sessionWrapper;
          } else {
            //status= COMPUTING it's being used for computing. computing is
            log.debug("session being used. waiting... current time {} ", time(timeSource, MILLISECONDS));
            try {
              lockObj.wait(10 * 1000);//wait for a max of 10 seconds
            } catch (InterruptedException e) {
              log.info("interrupted... ");
            }
            log.debug("out of waiting curr-time:{} time-elapsed {}", time(timeSource, MILLISECONDS), timeElapsed(timeSource, waitStart, MILLISECONDS));
            // now this thread has woken up because it got timed out after 10 seconds or it is notified after
            // the session was returned from another COMPUTING operation
            if (this.sessionWrapper.status == Status.UNUSED || this.sessionWrapper.status == Status.EXECUTING) {
              log.debug("Wait over. reusing the existing session ");
              this.sessionWrapper.status = Status.COMPUTING;
              return sessionWrapper;
            } else {
              //create a new Session
              return createSession(cloudManager);
            }
          }
        }
      }
    }

    private SessionWrapper createSession(SolrCloudManager cloudManager) throws InterruptedException, IOException {
      synchronized (lockObj) {
        log.debug("Creating a new session");
        Policy.Session session = cloudManager.getDistribStateManager().getAutoScalingConfig().getPolicy().createSession(cloudManager);
        log.debug("New session created ");
        this.sessionWrapper = new SessionWrapper(session, this);
        this.sessionWrapper.status = Status.COMPUTING;
        return sessionWrapper;
      }
    }


  }

  /**
   * How to get a shared Policy Session
   * 1) call {@link #getSession(SolrCloudManager)}
   * 2) compute all suggestions
   * 3) call {@link  SessionWrapper#returnSession(Policy.Session)}
   * 4) perform all suggestions
   * 5) call {@link  SessionWrapper#release()}
   */
  public static SessionWrapper getSession(SolrCloudManager cloudManager) throws IOException, InterruptedException {
    SessionRef sessionRef = (SessionRef) cloudManager.getObjectCache().computeIfAbsent(SessionRef.class.getName(), s -> new SessionRef());
    return sessionRef.get(cloudManager);
  }

  /**
   * Use this to get the last used session wrapper in this thread
   *
   * @param clear whether to unset the threadlocal or not
   */
  public static SessionWrapper getLastSessionWrapper(boolean clear) {
    SessionWrapper wrapper = SESSION_WRAPPPER_REF.get();
    if (clear) SESSION_WRAPPPER_REF.remove();
    return wrapper;

  }


  static ThreadLocal<SessionWrapper> SESSION_WRAPPPER_REF = new ThreadLocal<>();


  public static class SessionWrapper {
    public static final SessionWrapper DEFAULT_INSTANCE = new SessionWrapper(null, null);

    static {
      DEFAULT_INSTANCE.status = Status.NULL;
      DEFAULT_INSTANCE.createTime = -1L;
      DEFAULT_INSTANCE.lastUpdateTime = -1L;
    }

    private long createTime;
    private long lastUpdateTime;
    private Policy.Session session;
    public Status status;
    private final SessionRef ref;
    private AtomicInteger refCount = new AtomicInteger();
    public final long zkVersion;

    public long getCreateTime() {
      return createTime;
    }

    public long getLastUpdateTime() {
      return lastUpdateTime;
    }

    public SessionWrapper(Policy.Session session, SessionRef ref) {
      lastUpdateTime = createTime = session != null ?
          session.cloudManager.getTimeSource().getTimeNs() :
          TimeSource.NANO_TIME.getTimeNs();
      this.session = session;
      this.status = Status.UNUSED;
      this.ref = ref;
      this.zkVersion = session == null ?
          0 :
          session.getPolicy().zkVersion;
    }

    public Policy.Session get() {
      return session;
    }

    public SessionWrapper update(Policy.Session session) {
      this.lastUpdateTime = session != null ?
          session.cloudManager.getTimeSource().getTimeNs() :
          TimeSource.NANO_TIME.getTimeNs();
      this.session = session;
      return this;
    }

    public int getRefCount() {
      return refCount.get();
    }

    /**
     * return this for later use and update the session with the latest state
     * ensure that this is done after computing the suggestions
     */
    public void returnSession(Policy.Session session) {
      this.update(session);
      refCount.incrementAndGet();
      ref.returnSession(this);

    }

    //all ops are executed now it can be destroyed
    public void release() {
      refCount.decrementAndGet();
      ref.release(this);

    }
  }
}
