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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Factory is similar to {@link RecordingUpdateProcessorFactory}, but with the goal of
 * tracking requests across multiple collections/shards/replicas in a {@link SolrCloudTestCase}.
 * It can optionally save references to the commands it receives inm a single global
 * Map&lt;String,BlockingQueue&gt; keys in the map are arbitrary, but the intention is that tests
 * generate a key that is unique to that test, and configure the factory with the key as "group name"
 * to avoid cross talk between tests. Tests can poll for requests from a group to observe that the expected
 * commands are executed.  By default, this factory does nothing except return the "next"
 * processor from the chain unless it's told to {@link #startRecording(String)} in which case all factories
 * with the same group will begin recording.
 *
 * This class is only for unit test purposes and should not be used in any production capacity. It presumes all nodes
 * exist within the same JVM (i.e. {@link MiniSolrCloudCluster}).
 */
public final class TrackingUpdateProcessorFactory
    extends UpdateRequestProcessorFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String REQUEST_COUNT = "TrackingUpdateProcessorRequestCount";
  public static final String REQUEST_NODE = "TrackingUpdateProcessorRequestNode";

  /**
   * The map of group queues containing commands that were recorded
   * @see #startRecording
   */
  private final static Map<String, List<UpdateCommand>> groupToCommands = new ConcurrentHashMap<>();

  private String group = "default";

  public static void startRecording(String group) {
    final List<UpdateCommand> updateCommands = groupToCommands.get(group);
    assert updateCommands == null || updateCommands.isEmpty();

    List<UpdateCommand> existing = groupToCommands.put(group, Collections.synchronizedList(new ArrayList<>()));
    assert existing == null : "Test cross-talk?";
  }

  /**
   *
   * @param group the name of the group to fetch
   * @return A cloned queue containing the same elements as the queue held in groupToCommands
   */
  public static List<UpdateCommand> stopRecording(String group) {
    List<UpdateCommand> commands = groupToCommands.remove(group);
    return Arrays.asList(commands.toArray(new UpdateCommand[0])); // safe copy. input list is synchronized
  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    if (args != null && args.indexOf("group",0) >= 0) {
      group = (String) args.get("group");
      log.debug("Init URP, group '{}'", group);
    } else {
      log.warn("TrackingUpdateProcessorFactory initialized without group configuration, using 'default' but this group is shared" +
          "across the entire VM and guaranteed to have unpredictable behavior if used by more than one test");
    }
  }

  @Override
  @SuppressWarnings("resource")
  public synchronized UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                                         SolrQueryResponse rsp, 
                                                         UpdateRequestProcessor next ) {
    final List<UpdateCommand> commands = groupToCommands.get(group);
    return commands == null ? next : new RecordingUpdateRequestProcessor(commands, next);
  }

  private static final class RecordingUpdateRequestProcessor
      extends UpdateRequestProcessor {

    private final List<UpdateCommand> groupCommands;

    RecordingUpdateRequestProcessor(List<UpdateCommand> groupCommands, UpdateRequestProcessor next) {
      super(next);
      this.groupCommands = groupCommands;
    }

    private void record(UpdateCommand cmd) {
      groupCommands.add(cmd.clone()); // important because cmd.clear() will be called

      Map<Object, Object> context = cmd.getReq().getContext();
      context.put(REQUEST_COUNT, groupCommands.size());
      context.put(REQUEST_NODE, cmd.getReq().getCore().getName());
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      record(cmd);
      super.processAdd(cmd);
    }
    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      record(cmd);
      super.processDelete(cmd);
    }
    @Override
    public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
      record(cmd);
      super.processMergeIndexes(cmd);
    }
    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      record(cmd);
      super.processCommit(cmd);
    }
    @Override
    public void processRollback(RollbackUpdateCommand cmd) throws IOException {
      record(cmd);
      super.processRollback(cmd);
    }
  }
}



