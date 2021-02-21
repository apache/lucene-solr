
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

package org.apache.solr.cloud.api.collections;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.Timer;
import org.apache.solr.cloud.OverseerTaskProcessor;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.util.stats.MetricUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command returns stats about the Overseer, the cluster state updater and collection API activity occurring
 * <b>within the current Overseer node</b> (this is important because distributed operations occurring on other nodes
 * are <b>not included</b> in these stats, for example distributed cluster state updates or Per Replica States updates).<p>
 *
 * The structure of the returned results is as follows:
 * <ul>
 *   <li><b>{@code leader}:</b> {@code ID} of the current overseer leader node</li>
 *   <li><b>{@code overseer_queue_size}:</b> count of entries in the {@code /overseer/queue} Zookeeper queue/directory</li>
 *   <li><b>{@code overseer_work_queue_size}:</b> count of entries in the {@code /overseer/queue-work} Zookeeper queue/directory</li>
 *   <li><b>{@code overseer_collection_queue_size}:</b> count of entries in the {@code /overseer/collection-queue-work}
 *   Zookeeper queue/directory</li>
 *   <li><b>{@code overseer_operations}:</b> map (of maps) of success and error counts for operations. The operations
 *   (keys) tracked in this map are:
 *     <ul>
 *       <li>{@code am_i_leader} (Overseer checking it is still the elected Overseer as it processes cluster state update
 *       messages)</li>
 *       <li>{@code configset_}<i>{@code <config set operation>}</i> (from
 *       {@link org.apache.solr.handler.admin.ConfigSetsHandler.ConfigSetOperation})</li>
 *       <li>Cluster state change operation names from {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
 *       (not all of them!) and {@link org.apache.solr.cloud.overseer.OverseerAction} (the complete list: {@code create},
 *       {@code delete}, {@code createshard}, {@code deleteshard}, {@code addreplica}, {@code addreplicaprop}, {@code deletereplicaprop},
 *       {@code balanceshardunique}, {@code modifycollection}, {@code state}, {@code leader}, {@code deletecore}, {@code addroutingrule},
 *       {@code removeroutingrule}, {@code updateshardstate}, {@code downnode} and {@code quit} with this like one unlikely
 *       to be observed since the Overseer is existing right away)</li>
 *       <li>{@code update_state} (when Overseer cluster state updater persists changes in Zookeeper)</li>
 *     </ul>
 *     For each key, the value is a map composed of:
 *     <ul>
 *       <li>{@code requests}: success count of the given operation </li>
 *       <li>{@code errors}: error count of the operation </li>
 *       <li>More metrics (see below)</li>
 *     </ul>
 *   </li>
 *   <li><b>{@code collection_operations}:</b> map (of maps) of success and error counts for collection related operations.
 *   The operations(keys) tracked in this map are <b>all operations</b> that start with {@code collection_}, but the
 *   {@code collection_} prefix is <b>stripped</b> of the returned value. Possible keys are therefore:
 *     <ul>
 *       <li>{@code am_i_leader}: originating in a stat called {@code collection_am_i_leader} representing Overseer checking
 *       it is still the elected Overseer as it processes Collection API and Config Set API messages.</li>
 *       <li>Collection API operation names from {@link org.apache.solr.common.params.CollectionParams.CollectionAction} (the
 *       stripped {@code collection_} prefix gets added in {@link OverseerCollectionMessageHandler#getTimerName(String)})</li>
 *     </ul>
 *     For each key, the value is a map composed of:
 *     <ul>
 *       <li>{@code requests}: success count of the given operation </li>
 *       <li>{@code errors}: error count of the operation </li>
 *       <li>{@code recent_failures}: an <b>optional</b> entry containing a list of maps, each map having two entries, one
 *       with key {@code request} with a failed request properties (a {@link ZkNodeProps}) and the other with key
 *       {@code response} with the corresponding response properties (a {@link org.apache.solr.client.solrj.SolrResponse}).</li>
 *       <li>More metrics (see below)</li>
 *     </ul>
 *   </li>
 *   <li><b>{@code overseer_queue}:</b> metrics on operations done on the Zookeeper queue {@code /overseer/queue} (see
 *   metrics below).<br>
 *   The operations that can be done on the queue and that can be keys whose values are a metrics map are:
 *   <ul>
 *     <li>{@code offer}</li>
 *     <li>{@code peek}</li>
 *     <li>{@code peek_wait}</li>
 *     <li>{@code peek_wait_forever}</li>
 *     <li>{@code peekTopN_wait}</li>
 *     <li>{@code peekTopN_wait_forever}</li>
 *     <li>{@code poll}</li>
 *     <li>{@code remove}</li>
 *     <li>{@code remove_event}</li>
 *     <li>{@code take}</li>
 *   </ul>
 *   </li>
 *   <li><b>{@code overseer_internal_queue}:</b> same as above but for queue {@code /overseer/queue-work}</li>
 *   <li><b>{@code collection_queue}:</b> same as above but for queue {@code /overseer/collection-queue-work}</li>
 * </ul>
 *
 * <p>
 * Maps returned as values of keys in <b>{@code overseer_operations}</b>, <b>{@code collection_operations}</b>,
 * <b>{@code overseer_queue}</b>, <b>{@code overseer_internal_queue}</b> and <b>{@code collection_queue}</b> include
 * additional stats. These stats are provided by {@link MetricUtils}, and represent metrics on each type of operation
 * execution (be it failed or successful), see calls to {@link Stats#time(String)}. The metric keys are:
 * <ul>
 *       <li>{@code avgRequestsPerSecond}</li>
 *       <li>{@code 5minRateRequestsPerSecond}</li>
 *       <li>{@code 15minRateRequestsPerSecond}</li>
 *       <li>{@code avgTimePerRequest}</li>
 *       <li>{@code medianRequestTime}</li>
 *       <li>{@code 75thPcRequestTime}</li>
 *       <li>{@code 95thPcRequestTime}</li>
 *       <li>{@code 99thPcRequestTime}</li>
 *       <li>{@code 999thPcRequestTime}</li>
 * </ul>
 */
public class OverseerStatusCmd implements CollApiCmds.CollectionApiCommand {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final CollectionCommandContext ccc;

  public OverseerStatusCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    ZkStateReader zkStateReader = ccc.getZkStateReader();
    String leaderNode = OverseerTaskProcessor.getLeaderNode(zkStateReader.getZkClient());
    results.add("leader", leaderNode);
    Stat stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/queue",null, stat, true);
    results.add("overseer_queue_size", stat.getNumChildren());
    stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/queue-work",null, stat, true);
    results.add("overseer_work_queue_size", stat.getNumChildren());
    stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/collection-queue-work",null, stat, true);
    results.add("overseer_collection_queue_size", stat.getNumChildren());

    @SuppressWarnings({"rawtypes"})
    NamedList overseerStats = new NamedList();
    @SuppressWarnings({"rawtypes"})
    NamedList collectionStats = new NamedList();
    @SuppressWarnings({"rawtypes"})
    NamedList stateUpdateQueueStats = new NamedList();
    @SuppressWarnings({"rawtypes"})
    NamedList workQueueStats = new NamedList();
    @SuppressWarnings({"rawtypes"})
    NamedList collectionQueueStats = new NamedList();
    Stats stats = ccc.getOverseerStats();
    for (Map.Entry<String, Stats.Stat> entry : stats.getStats().entrySet()) {
      String key = entry.getKey();
      NamedList<Object> lst = new SimpleOrderedMap<>();
      if (key.startsWith("collection_")) {
        collectionStats.add(key.substring(11), lst);
        int successes = stats.getSuccessCount(entry.getKey());
        int errors = stats.getErrorCount(entry.getKey());
        lst.add("requests", successes);
        lst.add("errors", errors);
        List<Stats.FailedOp> failureDetails = stats.getFailureDetails(key);
        if (failureDetails != null) {
          List<SimpleOrderedMap<Object>> failures = new ArrayList<>();
          for (Stats.FailedOp failedOp : failureDetails) {
            SimpleOrderedMap<Object> fail = new SimpleOrderedMap<>();
            fail.add("request", failedOp.req.getProperties());
            fail.add("response", failedOp.resp.getResponse());
            failures.add(fail);
          }
          lst.add("recent_failures", failures);
        }
      } else if (key.startsWith("/overseer/queue_")) {
        stateUpdateQueueStats.add(key.substring(16), lst);
      } else if (key.startsWith("/overseer/queue-work_")) {
        workQueueStats.add(key.substring(21), lst);
      } else if (key.startsWith("/overseer/collection-queue-work_")) {
        collectionQueueStats.add(key.substring(32), lst);
      } else {
        // overseer stats
        overseerStats.add(key, lst);
        int successes = stats.getSuccessCount(entry.getKey());
        int errors = stats.getErrorCount(entry.getKey());
        lst.add("requests", successes);
        lst.add("errors", errors);
      }
      Timer timer = entry.getValue().requestTime;
      MetricUtils.addMetrics(lst, timer);
    }

    results.add("overseer_operations", overseerStats);
    results.add("collection_operations", collectionStats);
    results.add("overseer_queue", stateUpdateQueueStats);
    results.add("overseer_internal_queue", workQueueStats);
    results.add("collection_queue", collectionQueueStats);

  }
}
