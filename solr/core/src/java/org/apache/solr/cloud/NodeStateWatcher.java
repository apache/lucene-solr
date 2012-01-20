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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.common.cloud.CoreState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watcher for node state changes.
 */
public class NodeStateWatcher implements Watcher {

  private static Logger log = LoggerFactory.getLogger(NodeStateWatcher.class);

  public static interface NodeStateChangeListener {
    void coreChanged(String nodeName, Set<CoreState> states)
        throws KeeperException, InterruptedException;
  }

  private final SolrZkClient zkClient;
  private final String path;
  private volatile Set<CoreState> currentState = new HashSet<CoreState>();
  private final NodeStateChangeListener listener;
  private final String nodeName;

  
  public Set<CoreState> getCurrentState() {
    return currentState;
  }

  public NodeStateWatcher(SolrZkClient zkClient, String nodeName, String path,
      NodeStateChangeListener listener) throws KeeperException, InterruptedException {
    this.nodeName = nodeName;
    this.zkClient = zkClient;
    this.path = path;
    this.listener = listener;
    processStateChange();
  }

  @Override
  public void process(WatchedEvent event) {
    try {
      processStateChange();
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      return;
    } catch (Exception e) {
      log.warn("Error processing state change", e);
    } 
  }

  private void processStateChange() throws KeeperException, InterruptedException {
    byte[] data = zkClient.getData(path, this, null, true);

    if (data != null) {
        CoreState[] states = CoreState.load(data);
        List<CoreState> stateList = Arrays.asList(states);
        HashSet<CoreState> modifiedCores = new HashSet<CoreState>();
        modifiedCores.addAll(stateList);
        modifiedCores.removeAll(currentState);

        HashSet<CoreState> newState = new HashSet<CoreState>();
        newState.addAll(stateList);
        
        HashMap<String, CoreState> lookup = new HashMap<String, CoreState>();
        for(CoreState state: states) {
          lookup.put(state.getCoreName(), state);
        }

        //check for status change
        for(CoreState state: currentState) {
          if(lookup.containsKey(state.getCoreName())) {
            if(!state.getProperties().equals(lookup.get(state.getCoreName()).getProperties())) {
              modifiedCores.add(lookup.get(state.getCoreName()));
            }
          }
        }
        
        currentState = Collections.unmodifiableSet(newState);

        if (modifiedCores.size() > 0) {
          try {
            listener.coreChanged(nodeName, Collections.unmodifiableSet(modifiedCores));
          } catch (KeeperException e) {
            log.warn("Could not talk to ZK", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Could not talk to ZK", e);
          }
        }

    } else {
      // ignore null state
    }
  }
}
