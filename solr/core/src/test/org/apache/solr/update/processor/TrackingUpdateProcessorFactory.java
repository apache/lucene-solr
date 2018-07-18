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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.mina.util.ConcurrentHashSet;
import org.apache.solr.common.SolrException;
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
 * tracking requests across multiple collections/shards/replicas in a CloudSolrTestCase.
 * It can optionally save references to the commands it receives inm a single global
 * Map&lt;String,BlockingQueue&gt; keys in the map are arbitrary, but the intention is that tests
 * generate a key that is unique to that test, and configure the factory with the key as "group name"
 * to avoid cross talk between tests. Tests can poll for requests from a group to observe that the expected
 * commands are executed.  By default, this factory does nothing except return the "next"
 * processor from the chain unless it's told to {@link #startRecording()} in which case all factories
 * with the same group will begin recording. It is critical that tests utilizing this
 * processor call {@link #close()} on at least one group member after the test finishes. The requests associated with
 * the commands are also provided with a
 *
 * This class is only for unit test purposes and should not be used in any production capacity. It presumes all nodes
 * exist within the same JVM (i. e. MiniSolrCloudCluster).
 */
public final class TrackingUpdateProcessorFactory
  extends UpdateRequestProcessorFactory implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String REQUEST_COUNT = "TrackingUpdateProcessorRequestCount";
  public static final String REQUEST_NODE = "TrackingUpdateProcessorRequestNode";

  private final static Map<String,Set<TrackingUpdateProcessorFactory>> groupMembership = new ConcurrentHashMap<>();
  private final static Map<String,AtomicInteger> groupSerialNums = new ConcurrentHashMap<>();

  /**
   * The map of group queues containing commands that were recorded
   * @see #startRecording
   */
  private final static Map<String, List<UpdateCommand>> commandQueueMap = new ConcurrentHashMap<>();

  private static final Object memoryConsistency = new Object();

  private volatile boolean recording = false;

  private String group = "default";

  /**
   * Get a copy of the queue for the group.
   *
   * @param group the name of the group to fetch
   * @return A cloned queue containing the same elements as the queue held in commandQueueMap
   */
  public static ArrayList<UpdateCommand> commandsForGroup(String group) {
    synchronized (memoryConsistency) {
      return new ArrayList<>(commandQueueMap.get(group));
    }
  }

  public static void startRecording(String group) {
    synchronized (memoryConsistency) {
      Set<TrackingUpdateProcessorFactory> trackingUpdateProcessorFactories = groupMembership.get(group);
      if (trackingUpdateProcessorFactories == null || trackingUpdateProcessorFactories.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "There are no trackingUpdateProcessors for group " + group);
      }
      for (TrackingUpdateProcessorFactory trackingUpdateProcessorFactory : trackingUpdateProcessorFactories) {
        trackingUpdateProcessorFactory.startRecording();
      }
    }
  }
  public static void stopRecording(String group) {
    synchronized (memoryConsistency) {
      Set<TrackingUpdateProcessorFactory> trackingUpdateProcessorFactories = groupMembership.get(group);
      if (trackingUpdateProcessorFactories == null || trackingUpdateProcessorFactories.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "There are no trackingUpdateProcessors for group "
            + group + " available groups are:" + groupMembership.keySet());
      }
      for (TrackingUpdateProcessorFactory trackingUpdateProcessorFactory : trackingUpdateProcessorFactories) {
        trackingUpdateProcessorFactory.stopRecording();
      }
    }
  }

  @Override
  public void init(NamedList args) {
    if (args != null && args.indexOf("group",0) >= 0) {
      group = (String) args.get("group");
    } else {
      log.warn("TrackingUpdateProcessorFactory initialized without group configuration, using 'default' but this group is shared" +
          "across the entire VM and guaranteed to have unpredictable behavior if used by more than one test");
    }
    // compute if absent to avoid replacing in the case of multiple "default"
    commandQueueMap.computeIfAbsent(group, s -> new ArrayList<>());
    groupMembership.computeIfAbsent(group,s-> new ConcurrentHashSet<>());
    groupSerialNums.computeIfAbsent(group,s-> new AtomicInteger(0));

    groupMembership.get(group).add(this);
  }

  /**
   * @see #stopRecording 
   * @see #commandQueueMap
   */
  public synchronized void startRecording() {
    Set<TrackingUpdateProcessorFactory> facts = groupMembership.get(group);
    // facts being null is a bug, all instances should have a group.
    for (TrackingUpdateProcessorFactory fact : facts) {
      fact.recording = true;
    }
  }

  /** @see #startRecording */
  public synchronized void stopRecording() {
    Set<TrackingUpdateProcessorFactory> factories = groupMembership.get(group);
    // facts being null is a bug, all instances should have a group.
    for (TrackingUpdateProcessorFactory fact : factories) {
      fact.recording = false;
    }
  }

  @Override
  @SuppressWarnings("resource")
  public synchronized UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                                         SolrQueryResponse rsp, 
                                                         UpdateRequestProcessor next ) {
    return recording ? new RecordingUpdateRequestProcessor(group, next) : next;
  }

  @Override
  public void close() {
    commandQueueMap.remove(group);
    groupMembership.get(group).clear();
  }

  private static final class RecordingUpdateRequestProcessor
    extends UpdateRequestProcessor {

    private String group;

    public RecordingUpdateRequestProcessor(String group,
                                           UpdateRequestProcessor next) {
      super(next);
      this.group = group;
    }

    private void record(UpdateCommand cmd) {
      synchronized (memoryConsistency) {
        String coreName = cmd.getReq().getCore().getName();
        Map<Object, Object> context = cmd.getReq().getContext();
        context.put(REQUEST_COUNT, groupSerialNums.get(group).incrementAndGet());
        context.put(REQUEST_NODE, coreName);
        List<UpdateCommand> commands = commandQueueMap.get(group);
        commands.add(cmd.clone()); // important because cmd.clear() will be called
      }
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


    @Override
    protected void doClose() {
      super.doClose();
      groupMembership.get(group).remove(this);
    }
  }
}



