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
import java.util.IdentityHashMap;
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

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class PolicyHelper {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String POLICY_MAPPING_KEY = "PolicyHelper.policyMapping";

  @SuppressWarnings({"unchecked"})
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

    try {
      try {
        SESSION_WRAPPPER_REF.set(sessionWrapper = getSession(delegatingManager));
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "unable to get autoscaling policy session", e);
      }

      Policy.Session origSession = sessionWrapper.session;
      // new session needs to be created to avoid side-effects from per-collection policies
      // TODO: refactor so cluster state cache is separate from storage of policies to avoid per cluster vs per collection interactions
      // Need a Session that has all previous history of the original session, NOT filtered by what's present or not in Zookeeper
      // (as does constructor Session(SolrCloudManager, Policy, Transaction)).
      Policy.Session newSession = origSession.cloneToNewSession(delegatingManager);

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
        log.warn("Exception while reading disk free metric values for nodes to be used for collection: {}", collName, e);
      }


      Map<Replica.Type, Integer> typeVsCount = new EnumMap<>(Replica.Type.class);
      typeVsCount.put(Replica.Type.NRT, nrtReplicas);
      typeVsCount.put(Replica.Type.TLOG, tlogReplicas);
      typeVsCount.put(Replica.Type.PULL, pullReplicas);
      for (String shardName : shardNames) {
        int idx = 0;
        for (Map.Entry<Replica.Type, Integer> e : typeVsCount.entrySet()) {
          for (int i = 0; i < e.getValue(); i++) {
            Suggester suggester = newSession.getSuggester(ADDREPLICA)
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
            @SuppressWarnings({"rawtypes"})
            SolrRequest op = suggester.getSuggestion();
            if (op == null) {
              String errorId = "AutoScaling.error.diagnostics." + System.nanoTime();
              Policy.Session sessionCopy = suggester.session;
              log.error("errorId : {} {}", errorId
                  , handleExp(log, "", () -> Utils.writeJson(getDiagnostics(sessionCopy), new StringWriter(), true).toString())); // logOk

              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, " No node can satisfy the rules " +
                  Utils.toJSONString(Utils.getDeepCopy(newSession.expandedClauses, 4, true) + " More details from logs in node : "
                      + Utils.getMDCNode() + ", errorId : " + errorId));
            }
            newSession = suggester.getSession();
            positions.add(new ReplicaPosition(shardName, ++idx, e.getKey(), op.getParams().get(NODE)));
          }
        }
      }

      // We're happy with the updated session based on the original one, so let's update what the wrapper would hand
      // to the next computation that wants a session.
      sessionWrapper.update(newSession);
    } finally {
      policyMapping.remove();
      // We mark the wrapper (and its session) as being available to others.
      if (sessionWrapper != null) {
        sessionWrapper.returnSession();
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
    List<String> types = t == null? Collections.emptyList(): Arrays.asList(t);

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

  @SuppressWarnings({"unchecked"})
  private static void addMissingReplicas(ReplicaCount count, DocCollection coll, String shard, Replica.Type type, Suggestion.Ctx ctx) {
    int delta = count.delta(coll.getExpectedReplicaCount(type, 0), type);
    for (; ; ) {
      if (!ctx.needMore()) return;
      if (delta >= 0) break;
      @SuppressWarnings({"rawtypes"})
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
        if (log.isTraceEnabled()) {
          log.trace("LOGSTATE: {}",
              Utils.writeJson(loggingInfo(cloudManager.getDistribStateManager().getAutoScalingConfig().getPolicy(), cloudManager, suggester),
                  new StringWriter(), true));
        }
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
    /**
     * A command is actively using and modifying the session to compute placements
     */
    COMPUTING,
    /**
     * A command is not done yet processing its changes but no longer updates or even uses the session
     */
    EXECUTING
  }

  /**
   * This class stores sessions for sharing purposes. If a process requires a session to
   * compute operations:
   * <ol>
   * <li>see if there is an available non expired session in the cache,</li>
   * <li>if yes, borrow it.</li>
   * <li>if no, create a new one and borrow it.</li>
   * <li>after computing (update) operations are done, {@link #returnSession(SessionWrapper)} back to the cache so it's
   * again available for borrowing.</li>
   * <li>after all borrowers are done computing then executing with the session, {@link #release(SessionWrapper)} it,
   * which removes it from the cache.</li>
   * </ol>
   */
  static class SessionRef {
    /**
     * Lock protecting access to {@link #sessionWrapperSet} and to {@link #creationsInProgress}
     */
    private final Object lockObj = new Object();

    /**
     * Sessions currently in use in {@link Status#COMPUTING} or {@link Status#EXECUTING} states. As soon as all
     * uses of a session are over, that session is removed from this set. Sessions not actively in use are NOT kept around.
     *
     * <p>Access should only be done under the protection of {@link #lockObj}</p>
     */
    private Set<SessionWrapper> sessionWrapperSet = Collections.newSetFromMap(new IdentityHashMap<>());


    /**
     * Number of sessions currently being created but not yet present in {@link #sessionWrapperSet}.
     *
     * <p>Access should only be done under the protection of {@link #lockObj}</p>
     */
    private int creationsInProgress = 0;

    public SessionRef() {
    }

    // used only by tests
    boolean isEmpty() {
      synchronized (lockObj) {
        return sessionWrapperSet.isEmpty();
      }
    }

    /**
     * All operations suggested by the current session object
     * is complete. Do not even cache anything
     */
    private void release(SessionWrapper sessionWrapper) {
      boolean present;
      synchronized (lockObj) {
        present = sessionWrapperSet.remove(sessionWrapper);
      }
      if (!present) {
        log.warn("released session {} not found in session set", sessionWrapper.getCreateTime());
      } else {
        if (log.isDebugEnabled()) {
          TimeSource timeSource = sessionWrapper.session.cloudManager.getTimeSource();
          log.debug("final release, session {} lived a total of {}ms, ", sessionWrapper.getCreateTime(),
              timeElapsed(timeSource, TimeUnit.MILLISECONDS.convert(sessionWrapper.getCreateTime(),
                  TimeUnit.NANOSECONDS), MILLISECONDS)); // logOk
        }
      }
    }

    /**
     * Computing is over for this session and it may contain a new session with new state
     * The session can be used by others while the caller is performing operations
     */
    private void returnSession(SessionWrapper sessionWrapper) {
      boolean present;
      synchronized (lockObj) {
        sessionWrapper.status = Status.EXECUTING;
        present = sessionWrapperSet.contains(sessionWrapper);

        // wake up single thread waiting for a session return (ok if not woken up, wait is short)
        // Important to wake up a single one, otherwise of multiple waiting threads, all but one will immediately create new sessions
        lockObj.notify();
      }

      // Logging
      if (present) {
        if (log.isDebugEnabled()) {
          log.debug("returnSession {}", sessionWrapper.getCreateTime());
        }
      } else {
        log.warn("returning unknown session {} ", sessionWrapper.getCreateTime());
      }
    }

    /**
     * <p>Method returning an available session that can be used for {@link Status#COMPUTING}, either from the
     * {@link #sessionWrapperSet} cache or by creating a new one. The status of the returned session is set to {@link Status#COMPUTING}.</p>
     *
     * Some waiting is done in two cases:
     * <ul>
     *   <li>A candidate session is present in {@link #sessionWrapperSet} but is still {@link Status#COMPUTING}, a random wait
     *   is observed to see if the session gets freed to save a session creation and allow session reuse,</li>
     *   <li>It is necessary to create a new session but there are already sessions in the process of being created, a
     *   random wait is observed (if no waiting already occurred waiting for a session to become free) before creation
     *   takes place, just in case one of the created sessions got used then {@link #returnSession(SessionWrapper)} in the meantime.</li>
     * </ul>
     *
     * The random wait prevents the "thundering herd" effect when all threads needing a session at the same time create a new
     * one even though some differentiated waits could have led to better reuse and less session creations.
     *
     * @param allowWait usually <code>true</code> except in tests that know there's no point in waiting because nothing
     *                  will happen...
     */
    public SessionWrapper get(SolrCloudManager cloudManager, boolean allowWait) throws IOException, InterruptedException {
      TimeSource timeSource = cloudManager.getTimeSource();
      long oldestUpdateTimeNs = TimeUnit.SECONDS.convert(timeSource.getTimeNs(), TimeUnit.NANOSECONDS) - SESSION_EXPIRY;
      int zkVersion = cloudManager.getDistribStateManager().getAutoScalingConfig().getZkVersion();

      synchronized (lockObj) {
        SessionWrapper sw = getAvailableSession(zkVersion, oldestUpdateTimeNs);

        // Best case scenario: an available session
        if (sw != null) {
          if (log.isDebugEnabled()) {
            log.debug("reusing session {}", sw.getCreateTime());
          }
          return sw;
        }

        // Wait for a while before deciding what to do if waiting could help...
        if ((creationsInProgress != 0 || hasCandidateSession(zkVersion, oldestUpdateTimeNs)) && allowWait) {
          // Either an existing session might be returned and become usable while we wait, or a session in the process of being
          // created might finish creation, be used then returned and become usable. So we wait.
          // wait 1 to 10 secs. Random to help spread wakeups.
          long waitForMs = (long) (Math.random() * 9 * 1000) + 1000;

          if (log.isDebugEnabled()) {
            log.debug("No sessions are available, all busy COMPUTING (or {} creations in progress). starting wait of {}ms",
                creationsInProgress, waitForMs);
          }
          long waitStart = time(timeSource, MILLISECONDS);
          try {
            lockObj.wait(waitForMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          if (log.isDebugEnabled()) {
            log.debug("out of waiting. wait of {}ms, actual time elapsed {}ms", waitForMs, timeElapsed(timeSource, waitStart, MILLISECONDS));
          }

          // We've waited, now we can either reuse immediately an available session, or immediately create a new one
          sw = getAvailableSession(zkVersion, oldestUpdateTimeNs);

          // Second best case scenario: an available session
          if (sw != null) {
            if (log.isDebugEnabled()) {
              log.debug("reusing session {} after wait", sw.getCreateTime());
            }
            return sw;
          }
        }

        // We're going to create a new Session OUTSIDE of the critical section because session creation can take quite some time
        creationsInProgress++;
      }

      SessionWrapper newSessionWrapper = null;
      try {
        if (log.isDebugEnabled()) {
          log.debug("Creating a new session");
        }
        Policy.Session session = cloudManager.getDistribStateManager().getAutoScalingConfig().getPolicy().createSession(cloudManager);
        newSessionWrapper = new SessionWrapper(session, this);
        if (log.isDebugEnabled()) {
          log.debug("New session created, {}", newSessionWrapper.getCreateTime());
        }
        return newSessionWrapper;
      } finally {
        synchronized (lockObj) {
          creationsInProgress--;

          if (newSessionWrapper != null) {
            // Session created successfully
            sessionWrapperSet.add(newSessionWrapper);
          }
        }
      }
    }

    /**
       * Returns an available session from the cache (the best one once cache strategies are defined), or null if no session
       * from the cache is available (i.e. all are still COMPUTING, are too old, wrong zk version or the cache is empty).<p>
       * This method must be called while holding the monitor on {@link #lockObj}.<p>
       * The method updates the session status to computing.
       */
    private SessionWrapper getAvailableSession(int zkVersion, long oldestUpdateTimeNs) {
      for (SessionWrapper sw : sessionWrapperSet) {
        if (sw.status == Status.EXECUTING && sw.getLastUpdateTime() >= oldestUpdateTimeNs && sw.zkVersion == zkVersion) {
          sw.status = Status.COMPUTING;
          return sw;
        }
      }
      return null;
    }

    /**
     * Returns true if there's a session in the cache that could be returned (if it was free). This is required to
     * know if there's any point in waiting or if a new session should better be created right away.
     */
    private boolean hasCandidateSession(int zkVersion, long oldestUpdateTimeNs) {
      for (SessionWrapper sw : sessionWrapperSet) {
        if (sw.getLastUpdateTime() >= oldestUpdateTimeNs && sw.zkVersion == zkVersion) {
          return true;
        }
      }
      return false;
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
    return getSession(cloudManager, true);
  }

  static SessionWrapper getSession(SolrCloudManager cloudManager, boolean allowWait) throws IOException, InterruptedException {
    SessionRef sessionRef = (SessionRef) cloudManager.getObjectCache().computeIfAbsent(SessionRef.class.getName(), s -> new SessionRef());
    return sessionRef.get(cloudManager, allowWait);
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
    private final long createTime;
    private long lastUpdateTime;
    private Policy.Session session;
    public Status status;
    private final SessionRef ref;
    /**
     * Number of commands currently using the session in {@link Status#EXECUTING}. There is one <b>additional</b> command
     * using the session and updating it if {@link #status} is {@link Status#COMPUTING}
     */
    private final AtomicInteger refCount = new AtomicInteger();
    public final long zkVersion;

    /**
     * Nanoseconds (since/to some arbitrary time) when the session got created. Also used in logs (only in logs!) to identify the session.
     */
    public long getCreateTime() {
      return createTime;
    }

    public long getLastUpdateTime() {
      return lastUpdateTime;
    }

    public SessionWrapper(Policy.Session session, SessionRef ref) {
      createTime = session.cloudManager.getTimeSource().getTimeNs();
      lastUpdateTime = createTime;
      this.session = session;
      this.status = Status.COMPUTING; // Created for being used, so COMPUTING right away
      this.ref = ref;
      this.zkVersion = session.getPolicy().getZkVersion();
    }

    public Policy.Session get() {
      return session;
    }

    public void update(Policy.Session session) {
      // JMM multithreaded access issue on lastUpdateTime.
      this.lastUpdateTime = session.cloudManager.getTimeSource().getTimeNs();
      this.session = session;
    }

    public int getRefCount() {
      return refCount.get();
    }

    /**
     * return this for later use and update the session with the latest state
     * ensure that this is done after computing the suggestions
     */
    public void returnSession(Policy.Session session) {
      if (this.status != Status.COMPUTING) {
        log.warn("returning session {} not in state COMPUTING", this.getCreateTime());
      }

      this.update(session);
      this.returnSession();
    }

    /**
     * return this for later use without updating the internal Session for cases where it's easier to update separately
     */
    public void returnSession() {
      refCount.incrementAndGet();
      ref.returnSession(this);
    }

    //all ops are executed now it can be destroyed
    public void release() {
      if (refCount.decrementAndGet() <= 0) {
        ref.release(this);
      }
    }
  }
}
