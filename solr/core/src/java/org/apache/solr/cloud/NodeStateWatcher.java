package org.apache.solr.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Watcher for node state changes.
 */
public class NodeStateWatcher implements Watcher {

  public static interface NodeStateChangeListener {
    void coreCreated(String shardZkNodeName, Set<CoreState> cores) throws IOException, KeeperException;

    void coreDeleted(String shardZkNodeName, Set<CoreState> cores);

    void coreChanged(String nodeName, Set<CoreState> cores) throws IOException, KeeperException;
  }

  private final SolrZkClient zkClient;
  private boolean stop = false;
  private final String path;
  private volatile Set<CoreState> currentState = new HashSet<CoreState>();
  private final NodeStateChangeListener listener;
  private final String nodeName;

  
  public Set<CoreState> getCurrentState() {
    return currentState;
  }

  public NodeStateWatcher(SolrZkClient zkClient, String nodeName, String path,
      NodeStateChangeListener listener) {
    this.nodeName = nodeName;
    this.zkClient = zkClient;
    this.path = path;
    this.listener = listener;
  }

  public void close() {
    stop = true;
  }

  @Override
  public void process(WatchedEvent event) {
    if (stop)
      return;
    try {
      byte[] data = zkClient.getData(path, this, null);
      processStateChange(data);
    } catch (KeeperException e) {
      //stop working on any keeper error
      e.printStackTrace();
      stop = true;
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
  }

  void processStateChange(byte[] data) {
    if (data != null) {
      try {
        CoreState[] states = CoreState.fromBytes(data);
        List<CoreState> stateList = Arrays.asList(states);
        
        // get new cores:
        HashSet<CoreState> newCores = new HashSet<CoreState>();
        newCores.addAll(stateList);
        newCores.removeAll(currentState);

        HashSet<CoreState> deadCores = new HashSet<CoreState>();
        deadCores.addAll(currentState);
        deadCores.removeAll(stateList);

        HashSet<CoreState> newState = new HashSet<CoreState>();
        newState.addAll(stateList);
        
        HashMap<String, CoreState> lookup = new HashMap<String, CoreState>();
        for(CoreState state: states) {
          lookup.put(state.getCoreName(), state);
        }

        HashSet<CoreState> changedCores = new HashSet<CoreState>();

        //check for status change
        for(CoreState state: currentState) {
          if(lookup.containsKey(state.getCoreName())) {
            if(!state.getProperties().equals(lookup.get(state.getCoreName()).getProperties())) {
              changedCores.add(lookup.get(state.getCoreName()));
            }
          }
        }
        
        currentState = Collections.unmodifiableSet(newState);

        if (newCores.size() > 0) {
          try {
            listener.coreCreated(nodeName, Collections.unmodifiableSet(newCores));
          } catch (KeeperException e) {
            //zk error, stop
            stop=true;
          }
        }
        if (deadCores.size() > 0) {
          listener.coreDeleted(nodeName, Collections.unmodifiableSet(deadCores));
        }

        if (changedCores.size() > 0) {
          try {
          listener.coreChanged(nodeName, Collections.unmodifiableSet(changedCores));
          } catch (KeeperException e) {
            //zk error, stop
            stop=true;
          }
        }


      } catch (IOException e) {
        e.printStackTrace();
      }

    } else {
      // ignore null state
    }
  }
}
