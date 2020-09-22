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

package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Locale;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.update.SolrIndexSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.CORE_IDX;

/**
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class IndexSizeTrigger extends TriggerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // configuration properties
  public static final String ABOVE_BYTES_PROP = "aboveBytes";
  public static final String ABOVE_DOCS_PROP = "aboveDocs";
  public static final String ABOVE_OP_PROP = "aboveOp";
  public static final String BELOW_BYTES_PROP = "belowBytes";
  public static final String BELOW_DOCS_PROP = "belowDocs";
  public static final String BELOW_OP_PROP = "belowOp";
  public static final String COLLECTIONS_PROP = "collections";
  public static final String MAX_OPS_PROP = "maxOps";
  public static final String SPLIT_FUZZ_PROP = CommonAdminParams.SPLIT_FUZZ;
  public static final String SPLIT_METHOD_PROP = CommonAdminParams.SPLIT_METHOD;
  public static final String SPLIT_BY_PREFIX = CommonAdminParams.SPLIT_BY_PREFIX;

  // event properties
  public static final String BYTES_SIZE_KEY = "__bytes__";
  public static final String TOTAL_BYTES_SIZE_KEY = "__total_bytes__";
  public static final String DOCS_SIZE_KEY = "__docs__";
  public static final String MAX_DOC_KEY = "__maxDoc__";
  public static final String COMMIT_SIZE_KEY = "__commitBytes__";
  public static final String ABOVE_SIZE_KEY = "aboveSize";
  public static final String BELOW_SIZE_KEY = "belowSize";
  public static final String VIOLATION_KEY = "violationType";

  public static final int DEFAULT_MAX_OPS = 10;

  public enum Unit { bytes, docs }

  private long aboveBytes, aboveDocs, belowBytes, belowDocs;
  private int maxOps;
  private SolrIndexSplitter.SplitMethod splitMethod;
  private boolean splitByPrefix;
  private float splitFuzz;
  private CollectionParams.CollectionAction aboveOp, belowOp;
  private final Set<String> collections = new HashSet<>();
  private final Map<String, Long> lastAboveEventMap = new ConcurrentHashMap<>();
  private final Map<String, Long> lastBelowEventMap = new ConcurrentHashMap<>();

  public IndexSizeTrigger(String name) {
    super(TriggerEventType.INDEXSIZE, name);
    TriggerUtils.validProperties(validProperties,
        ABOVE_BYTES_PROP, ABOVE_DOCS_PROP, ABOVE_OP_PROP,
        BELOW_BYTES_PROP, BELOW_DOCS_PROP, BELOW_OP_PROP,
        COLLECTIONS_PROP, MAX_OPS_PROP,
        SPLIT_METHOD_PROP, SPLIT_FUZZ_PROP, SPLIT_BY_PREFIX);
  }

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    super.configure(loader, cloudManager, properties);
    String aboveStr = String.valueOf(properties.getOrDefault(ABOVE_BYTES_PROP, Long.MAX_VALUE));
    String belowStr = String.valueOf(properties.getOrDefault(BELOW_BYTES_PROP, -1));
    try {
      aboveBytes = Long.parseLong(aboveStr);
      if (aboveBytes <= 0) {
        throw new Exception("value must be > 0");
      }
    } catch (Exception e) {
      throw new TriggerValidationException(getName(), ABOVE_BYTES_PROP, "invalid value '" + aboveStr + "': " + e.toString());
    }
    try {
      belowBytes = Long.parseLong(belowStr);
      if (belowBytes < 0) {
        belowBytes = -1;
      }
    } catch (Exception e) {
      throw new TriggerValidationException(getName(), BELOW_BYTES_PROP, "invalid value '" + belowStr + "': " + e.toString());
    }
    // below must be at least 2x smaller than above, otherwise splitting a shard
    // would immediately put the shard below the threshold and cause the mergeshards action
    if (belowBytes > 0 && (belowBytes * 2 > aboveBytes)) {
      throw new TriggerValidationException(getName(), BELOW_BYTES_PROP,
          "invalid value " + belowBytes + ", should be less than half of '" + ABOVE_BYTES_PROP + "' value, which is " + aboveBytes);
    }
    // do the same for docs bounds
    aboveStr = String.valueOf(properties.getOrDefault(ABOVE_DOCS_PROP, Long.MAX_VALUE));
    belowStr = String.valueOf(properties.getOrDefault(BELOW_DOCS_PROP, -1));
    try {
      aboveDocs = Long.parseLong(aboveStr);
      if (aboveDocs <= 0) {
        throw new Exception("value must be > 0");
      }
    } catch (Exception e) {
      throw new TriggerValidationException(getName(), ABOVE_DOCS_PROP, "invalid value '" + aboveStr + "': " + e.toString());
    }
    try {
      belowDocs = Long.parseLong(belowStr);
      if (belowDocs < 0) {
        belowDocs = -1;
      }
    } catch (Exception e) {
      throw new TriggerValidationException(getName(), BELOW_DOCS_PROP, "invalid value '" + belowStr + "': " + e.toString());
    }
    // below must be at least 2x smaller than above, otherwise splitting a shard
    // would immediately put the shard below the threshold and cause the mergeshards action
    if (belowDocs > 0 && (belowDocs * 2 > aboveDocs)) {
      throw new TriggerValidationException(getName(), BELOW_DOCS_PROP,
          "invalid value " + belowDocs + ", should be less than half of '" + ABOVE_DOCS_PROP + "' value, which is " + aboveDocs);
    }

    String collectionsString = (String) properties.get(COLLECTIONS_PROP);
    if (collectionsString != null && !collectionsString.isEmpty()) {
      collections.addAll(StrUtils.splitSmart(collectionsString, ','));
    }
    String aboveOpStr = String.valueOf(properties.getOrDefault(ABOVE_OP_PROP, CollectionParams.CollectionAction.SPLITSHARD.toLower()));
    // TODO: this is a placeholder until SOLR-9407 is implemented
    String belowOpStr = String.valueOf(properties.getOrDefault(BELOW_OP_PROP, CollectionParams.CollectionAction.MERGESHARDS.toLower()));
    aboveOp = CollectionParams.CollectionAction.get(aboveOpStr);
    if (aboveOp == null) {
      throw new TriggerValidationException(getName(), ABOVE_OP_PROP, "unrecognized value of: '" + aboveOpStr + "'");
    }
    belowOp = CollectionParams.CollectionAction.get(belowOpStr);
    if (belowOp == null) {
      throw new TriggerValidationException(getName(), BELOW_OP_PROP, "unrecognized value of: '" + belowOpStr + "'");
    }
    String maxOpsStr = String.valueOf(properties.getOrDefault(MAX_OPS_PROP, DEFAULT_MAX_OPS));
    try {
      maxOps = Integer.parseInt(maxOpsStr);
      if (maxOps < 1) {
        throw new Exception("must be > 1");
      }
    } catch (Exception e) {
      throw new TriggerValidationException(getName(), MAX_OPS_PROP, "invalid value: '" + maxOpsStr + "': " + e.getMessage());
    }
    String methodStr = (String)properties.getOrDefault(SPLIT_METHOD_PROP, SolrIndexSplitter.SplitMethod.LINK.toLower());
    splitMethod = SolrIndexSplitter.SplitMethod.get(methodStr);
    if (splitMethod == null) {
      throw new TriggerValidationException(getName(), SPLIT_METHOD_PROP, "unrecognized value of: '" + methodStr + "'");
    }
    String fuzzStr = String.valueOf(properties.getOrDefault(SPLIT_FUZZ_PROP, 0.0f));
    try {
      splitFuzz = Float.parseFloat(fuzzStr);
    } catch (Exception e) {
      throw new TriggerValidationException(getName(), SPLIT_FUZZ_PROP, "invalid value: '" + fuzzStr + "': " + e.getMessage());
    }
    String splitByPrefixStr = String.valueOf(properties.getOrDefault(SPLIT_BY_PREFIX, false));
    try {
      splitByPrefix = getValidBool(splitByPrefixStr);
    } catch (Exception e) {
      throw new TriggerValidationException(getName(), SPLIT_BY_PREFIX, "invalid value: '" + splitByPrefixStr + "': " + e.getMessage());
    }
  }
  
  private boolean getValidBool(String str) throws Exception {
    if (str != null && (str.toLowerCase(Locale.ROOT).equals("true") || str.toLowerCase(Locale.ROOT).equals("false"))) {
      return Boolean.parseBoolean(str);
    }
    throw new IllegalArgumentException("Expected a valid boolean value but got " + str);
  }

  @Override
  protected Map<String, Object> getState() {
    Map<String, Object> state = new HashMap<>();
    state.put("lastAboveEventMap", lastAboveEventMap);
    state.put("lastBelowEventMap", lastBelowEventMap);
    return state;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  protected void setState(Map<String, Object> state) {
    this.lastAboveEventMap.clear();
    this.lastBelowEventMap.clear();
    Map<String, Long> replicaVsTime = (Map<String, Long>)state.get("lastAboveEventMap");
    if (replicaVsTime != null) {
      this.lastAboveEventMap.putAll(replicaVsTime);
    }
    replicaVsTime = (Map<String, Long>)state.get("lastBelowEventMap");
    if (replicaVsTime != null) {
      this.lastBelowEventMap.putAll(replicaVsTime);
    }
  }

  @Override
  public void restoreState(AutoScaling.Trigger old) {
    assert old.isClosed();
    if (old instanceof IndexSizeTrigger) {
      IndexSizeTrigger that = (IndexSizeTrigger)old;
      assert this.name.equals(that.name);
      this.lastAboveEventMap.clear();
      this.lastBelowEventMap.clear();
      this.lastAboveEventMap.putAll(that.lastAboveEventMap);
      this.lastBelowEventMap.putAll(that.lastBelowEventMap);
    } else {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE,
          "Unable to restore state from an unknown type of trigger");
    }
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void run() {
    synchronized(this) {
      if (isClosed) {
        log.warn("{} ran but was already closed", getName());
        return;
      }
    }
    AutoScaling.TriggerEventProcessor processor = processorRef.get();
    if (processor == null) {
      return;
    }

    // replica name / info + size, retrieved from leaders only
    Map<String, ReplicaInfo> currentSizes = new HashMap<>();

    try {
      ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
      for (String node : clusterState.getLiveNodes()) {
        Map<String, ReplicaInfo> metricTags = new HashMap<>();
        // coll, shard, replica
        Map<String, Map<String, List<ReplicaInfo>>> infos = cloudManager.getNodeStateProvider().getReplicaInfo(node, Collections.emptyList());
        infos.forEach((coll, shards) -> {
          if (!collections.isEmpty() && !collections.contains(coll)) {
            return;
          }
          DocCollection docCollection = clusterState.getCollection(coll);

          shards.forEach((sh, replicas) -> {
            // check only the leader replica in an active shard
            Slice s = docCollection.getSlice(sh);
            if (s.getState() != Slice.State.ACTIVE) {
              return;
            }
            Replica r = s.getLeader();
            // no leader - don't do anything
            if (r == null) {
              return;
            }
            // not on this node
            if (!r.getNodeName().equals(node)) {
              return;
            }
            // find ReplicaInfo
            ReplicaInfo info = null;
            for (ReplicaInfo ri : replicas) {
              if (r.getCoreName().equals(ri.getCore())) {
                info = ri;
                break;
              }
            }
            if (info == null) {
              // probably replica is not on this node?
              return;
            }
            // we have to translate to the metrics registry name, which uses "_replica_nN" as suffix
            String replicaName = Utils.parseMetricsReplicaName(coll, info.getCore());
            if (replicaName == null) { // should never happen???
              replicaName = info.getName(); // which is actually coreNode name...
            }
            String registry = SolrCoreMetricManager.createRegistryName(true, coll, sh, replicaName, null);
            String tag = "metrics:" + registry + ":" + CORE_IDX.metricsAttribute;
            metricTags.put(tag, info);
            tag = "metrics:" + registry + ":SEARCHER.searcher.numDocs";
            metricTags.put(tag, info);
            tag = "metrics:" + registry + ":SEARCHER.searcher.maxDoc";
            metricTags.put(tag, info);
            tag = "metrics:" + registry + ":SEARCHER.searcher.indexCommitSize";
            metricTags.put(tag, info);
          });
        });
        if (metricTags.isEmpty()) {
          continue;
        }
        Map<String, Object> sizes = cloudManager.getNodeStateProvider().getNodeValues(node, metricTags.keySet());
        sizes.forEach((tag, size) -> {
          final ReplicaInfo info = metricTags.get(tag);
          if (info == null) {
            log.warn("Missing replica info for response tag {}", tag);
          } else {
            // verify that it's a Number
            if (!(size instanceof Number)) {
              log.warn("invalid size value for tag {} - not a number: '{}' is {}", tag, size, size.getClass().getName());
              return;
            }

            ReplicaInfo currentInfo = currentSizes.computeIfAbsent(info.getCore(), k -> (ReplicaInfo)info.clone());
            if (tag.contains("INDEX")) {
              currentInfo.getVariables().put(TOTAL_BYTES_SIZE_KEY, ((Number) size).longValue());
            } else if (tag.endsWith("SEARCHER.searcher.numDocs")) {
              currentInfo.getVariables().put(DOCS_SIZE_KEY, ((Number) size).longValue());
            } else if (tag.endsWith("SEARCHER.searcher.maxDoc")) {
              currentInfo.getVariables().put(MAX_DOC_KEY, ((Number) size).longValue());
            } else if (tag.endsWith("SEARCHER.searcher.indexCommitSize")) {
              currentInfo.getVariables().put(COMMIT_SIZE_KEY, ((Number) size).longValue());
            }
          }
        });
      }
    } catch (IOException e) {
      log.warn("Error running trigger {}", getName(), e);
      return;
    }

    long now = cloudManager.getTimeSource().getTimeNs();

    // now check thresholds

    // collection / list(info)
    Map<String, List<ReplicaInfo>> aboveSize = new HashMap<>();

    Set<String> splittable = new HashSet<>();

    currentSizes.forEach((coreName, info) -> {
      // calculate estimated bytes
      long maxDoc = (Long)info.getVariable(MAX_DOC_KEY);
      long numDocs = (Long)info.getVariable(DOCS_SIZE_KEY);
      long commitSize = (Long)info.getVariable(COMMIT_SIZE_KEY, 0L);
      if (commitSize <= 0) {
        commitSize = (Long)info.getVariable(TOTAL_BYTES_SIZE_KEY);
      }
      // calculate estimated size as a side-effect
      commitSize = estimatedSize(maxDoc, numDocs, commitSize);
      info.getVariables().put(BYTES_SIZE_KEY, commitSize);

      if ((Long)info.getVariable(BYTES_SIZE_KEY) > aboveBytes ||
          (Long)info.getVariable(DOCS_SIZE_KEY) > aboveDocs) {
        if (waitForElapsed(coreName, now, lastAboveEventMap)) {
          List<ReplicaInfo> infos = aboveSize.computeIfAbsent(info.getCollection(), c -> new ArrayList<>());
          if (!infos.contains(info)) {
            if ((Long)info.getVariable(BYTES_SIZE_KEY) > aboveBytes) {
              info.getVariables().put(VIOLATION_KEY, ABOVE_BYTES_PROP);
            } else {
              info.getVariables().put(VIOLATION_KEY, ABOVE_DOCS_PROP);
            }
            infos.add(info);
            splittable.add(info.getName());
          }
        }
      } else {
        // no violation - clear waitForElapsed
        lastAboveEventMap.remove(coreName);
      }
    });

    // collection / list(info)
    Map<String, List<ReplicaInfo>> belowSize = new HashMap<>();

    currentSizes.forEach((coreName, info) -> {
      if (((Long)info.getVariable(BYTES_SIZE_KEY) < belowBytes ||
          (Long)info.getVariable(DOCS_SIZE_KEY) < belowDocs) &&
          // make sure we don't produce conflicting ops
          !splittable.contains(info.getName())) {
        if (waitForElapsed(coreName, now, lastBelowEventMap)) {
          List<ReplicaInfo> infos = belowSize.computeIfAbsent(info.getCollection(), c -> new ArrayList<>());
          if (!infos.contains(info)) {
            if ((Long)info.getVariable(BYTES_SIZE_KEY) < belowBytes) {
              info.getVariables().put(VIOLATION_KEY, BELOW_BYTES_PROP);
            } else {
              info.getVariables().put(VIOLATION_KEY, BELOW_DOCS_PROP);
            }
            infos.add(info);
          }
        }
      } else {
        // no violation - clear waitForElapsed
        lastBelowEventMap.remove(coreName);
      }
    });

    if (aboveSize.isEmpty() && belowSize.isEmpty()) {
      log.trace("NO VIOLATIONS: Now={}", now);
      log.trace("lastAbove={}", lastAboveEventMap);
      log.trace("lastBelow={}", lastBelowEventMap);
      return;
    }

    // find the earliest time when a condition was exceeded
    final AtomicLong eventTime = new AtomicLong(now);

    // calculate ops
    final List<TriggerEvent.Op> ops = new ArrayList<>();
    aboveSize.forEach((coll, replicas) -> {
      // sort by decreasing size to first split the largest ones
      // XXX see the comment below about using DOCS_SIZE_PROP in lieu of BYTES_SIZE_PROP
      replicas.sort((r1, r2) -> {
        long delta = (Long) r1.getVariable(DOCS_SIZE_KEY) - (Long) r2.getVariable(DOCS_SIZE_KEY);
        if (delta > 0) {
          return -1;
        } else if (delta < 0) {
          return 1;
        } else {
          return 0;
        }
      });
      replicas.forEach(r -> {
        if (ops.size() >= maxOps) {
          return;
        }
        TriggerEvent.Op op = new TriggerEvent.Op(aboveOp);
        op.addHint(Suggester.Hint.COLL_SHARD, new Pair<>(coll, r.getShard()));
        Map<String, Object> params = new HashMap<>();
        params.put(SPLIT_METHOD_PROP, splitMethod.toLower());
        if (splitFuzz > 0) {
          params.put(SPLIT_FUZZ_PROP, splitFuzz);
        }
        params.put(SPLIT_BY_PREFIX, splitByPrefix);
        op.addHint(Suggester.Hint.PARAMS, params);
        ops.add(op);
        Long time = lastAboveEventMap.get(r.getCore());
        if (time != null && eventTime.get() > time) {
          eventTime.set(time);
        }
      });
    });
    belowSize.forEach((coll, replicas) -> {
      if (replicas.size() < 2) {
        return;
      }
      if (ops.size() >= maxOps) {
        return;
      }
      // sort by increasing size
      replicas.sort((r1, r2) -> {
        // XXX this is not quite correct - if BYTES_SIZE_PROP decided that replica got here
        // then we should be sorting by BYTES_SIZE_PROP. However, since DOCS and BYTES are
        // loosely correlated it's simpler to sort just by docs (which better reflects the "too small"
        // condition than index size, due to possibly existing deleted docs that still occupy space)
        long delta = (Long) r1.getVariable(DOCS_SIZE_KEY) - (Long) r2.getVariable(DOCS_SIZE_KEY);
        if (delta > 0) {
          return 1;
        } else if (delta < 0) {
          return -1;
        } else {
          return 0;
        }
      });

      // TODO: MERGESHARDS is not implemented yet. For now take the top two smallest shards
      // TODO: but in the future we probably need to get ones with adjacent ranges.

      // TODO: generate as many MERGESHARDS as needed to consume all belowSize shards
      TriggerEvent.Op op = new TriggerEvent.Op(belowOp);
      op.addHint(Suggester.Hint.COLL_SHARD, new Pair(coll, replicas.get(0).getShard()));
      op.addHint(Suggester.Hint.COLL_SHARD, new Pair(coll, replicas.get(1).getShard()));
      ops.add(op);
      Long time = lastBelowEventMap.get(replicas.get(0).getCore());
      if (time != null && eventTime.get() > time) {
        eventTime.set(time);
      }
      time = lastBelowEventMap.get(replicas.get(1).getCore());
      if (time != null && eventTime.get() > time) {
        eventTime.set(time);
      }
    });

    if (ops.isEmpty()) {
      return;
    }
    if (processor.process(new IndexSizeEvent(getName(), eventTime.get(), ops, aboveSize, belowSize))) {
      // update last event times
      aboveSize.forEach((coll, replicas) -> {
        replicas.forEach(r -> lastAboveEventMap.put(r.getCore(), now));
      });
      belowSize.forEach((coll, replicas) -> {
        if (replicas.size() < 2) {
          return;
        }
        lastBelowEventMap.put(replicas.get(0).getCore(), now);
        lastBelowEventMap.put(replicas.get(1).getCore(), now);
      });
    }
  }

  public static long estimatedSize(long maxDoc, long numDocs, long commitSize) {
    if (maxDoc == 0) {
      return 0;
    }
    if (maxDoc == numDocs) {
      return commitSize;
    }
    return commitSize * numDocs / maxDoc;
  }

  private boolean waitForElapsed(String name, long now, Map<String, Long> lastEventMap) {
    Long lastTime = lastEventMap.computeIfAbsent(name, s -> now);
    long elapsed = TimeUnit.SECONDS.convert(now - lastTime, TimeUnit.NANOSECONDS);
    log.trace("name={}, lastTime={}, elapsed={}", name, lastTime, elapsed);
    if (TimeUnit.SECONDS.convert(now - lastTime, TimeUnit.NANOSECONDS) < getWaitForSecond()) {
      return false;
    }
    return true;
  }

  public static class IndexSizeEvent extends TriggerEvent {
    public IndexSizeEvent(String source, long eventTime, List<Op> ops, Map<String, List<ReplicaInfo>> aboveSize,
                          Map<String, List<ReplicaInfo>> belowSize) {
      super(TriggerEventType.INDEXSIZE, source, eventTime, null);
      properties.put(TriggerEvent.REQUESTED_OPS, ops);
      // avoid passing very large amounts of data here - just use replica names
      TreeMap<String, String> above = new TreeMap<>();
      aboveSize.forEach((coll, replicas) ->
          replicas.forEach(r -> above.put(r.getCore(), "docs=" + r.getVariable(DOCS_SIZE_KEY) + ", bytes=" + r.getVariable(BYTES_SIZE_KEY))));
      properties.put(ABOVE_SIZE_KEY, above);
      TreeMap<String, String> below = new TreeMap<>();
      belowSize.forEach((coll, replicas) ->
          replicas.forEach(r -> below.put(r.getCore(), "docs=" + r.getVariable(DOCS_SIZE_KEY) + ", bytes=" + r.getVariable(BYTES_SIZE_KEY))));
      properties.put(BELOW_SIZE_KEY, below);
    }
  }

}
