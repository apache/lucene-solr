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
package org.apache.solr.cloud;

import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StatePublisher implements Closeable {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  public static final String OPERATION = "op";

  private final Map<String,String> stateCache = new ConcurrentHashMap<>(32, 0.75f, 6);
  private final ZkStateReader zkStateReader;
  private final CoreContainer cc;

  public static class NoOpMessage extends ZkNodeProps {
  }

  public static final NoOpMessage TERMINATE_OP = new NoOpMessage();

  private final BlockingArrayQueue<ZkNodeProps> workQueue = new BlockingArrayQueue(64, 16);
  private final ZkDistributedQueue overseerJobQueue;
  private volatile Worker worker;
  private volatile Future<?> workerFuture;

  private volatile boolean terminated;
  private class Worker implements Runnable {

    Worker() {

    }

    @Override
    public void run() {
      ActionThrottle throttle = new ActionThrottle("StatePublisherWorker", 0);

      while (!terminated && !zkStateReader.getZkClient().isClosed()) {
        if (!zkStateReader.getZkClient().isConnected()) {
          try {
            Thread.sleep(250);
          } catch (InterruptedException e) {
            ParWork.propagateInterrupt(e, true);
            return;
          }
          continue;
        }
        throttle.minimumWaitBetweenActions();
        throttle.markAttemptingAction();
        ZkNodeProps message = null;
        ZkNodeProps bulkMessage = new ZkNodeProps();
        bulkMessage.getProperties().put(OPERATION, "state");
        try {
          try {
            message = workQueue.poll(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            ParWork.propagateInterrupt(e, true);
            return;
          }
          if (message != null) {
            if (log.isDebugEnabled()) log.debug("Got state message " + message);
            if (message == TERMINATE_OP) {
              terminated = true;
              message = null;
            } else {
              bulkMessage(message, bulkMessage);
            }

            while (message != null && !terminated) {
              try {
                message = workQueue.poll(15, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {

              }
              if (log.isDebugEnabled()) log.debug("Got state message " + message);
              if (message != null) {
                if (message == TERMINATE_OP) {
                  terminated = true;
                } else {
                  bulkMessage(message, bulkMessage);
                }
              }
            }
            processMessage(bulkMessage);
          }

        } catch (AlreadyClosedException e) {
          log.info("StatePublisher run loop hit AlreadyClosedException, exiting ...");
          return;
        } catch (Exception e) {
          log.error("Exception in StatePublisher run loop, exiting", e);
          return;
        }
      }
    }

    private void bulkMessage(ZkNodeProps zkNodeProps, ZkNodeProps bulkMessage) throws KeeperException, InterruptedException {
      if (OverseerAction.get(zkNodeProps.getStr(OPERATION)) == OverseerAction.DOWNNODE) {
        String nodeName = zkNodeProps.getStr(ZkStateReader.NODE_NAME_PROP);
        bulkMessage.getProperties().put(OverseerAction.DOWNNODE.toLower(), nodeName);

        clearStatesForNode(bulkMessage, nodeName);
      } else if (OverseerAction.get(zkNodeProps.getStr(OPERATION)) == OverseerAction.RECOVERYNODE) {
        String nodeName = zkNodeProps.getStr(ZkStateReader.NODE_NAME_PROP);
        bulkMessage.getProperties().put(OverseerAction.RECOVERYNODE.toLower(), nodeName);

        clearStatesForNode(bulkMessage, nodeName);
      } else {
        String collection = zkNodeProps.getStr(ZkStateReader.COLLECTION_PROP);
        String core = zkNodeProps.getStr(ZkStateReader.CORE_NAME_PROP);
        String state = zkNodeProps.getStr(ZkStateReader.STATE_PROP);

        if (collection == null || core == null || state == null) {
          log.error("Bad state found for publish! {} {}", zkNodeProps, bulkMessage);
          return;
        }
        String line = collection + "," + Replica.State.getShortState(Replica.State.valueOf(state.toUpperCase(Locale.ROOT)));
        if (log.isDebugEnabled()) log.debug("Bulk publish core={} line={}", core, line);
        bulkMessage.getProperties().put(core, line);
      }
    }

    private void clearStatesForNode(ZkNodeProps bulkMessage, String nodeName) {
      Set<String> removeCores = new HashSet<>();
      Set<String> cores = bulkMessage.getProperties().keySet();
      for (String core : cores) {
        if (core.equals(OverseerAction.DOWNNODE.toLower()) || core.equals(OverseerAction.RECOVERYNODE.toLower())) {
          continue;
        }
        Collection<DocCollection> collections = zkStateReader.getClusterState().getCollectionsMap().values();
        for (DocCollection collection : collections) {
          Replica replica = collection.getReplica(core);
          if (replica != null) {
            if (replica.getNodeName().equals(nodeName)) {
              removeCores.add(core);
            }
          }
        }

      }
      for (String core : removeCores) {
        bulkMessage.getProperties().remove(core);
      }
    }

    private void processMessage(ZkNodeProps message) throws KeeperException, InterruptedException {
      byte[] updates = Utils.toJSON(message);
      if (log.isDebugEnabled()) log.debug("Send state updates to Overseer {}", message);
      overseerJobQueue.offer(updates);
    }
  }

  public StatePublisher(ZkDistributedQueue overseerJobQueue, ZkStateReader zkStateReader, CoreContainer cc) {
    this.overseerJobQueue = overseerJobQueue;
    this.zkStateReader = zkStateReader;
    this.cc = cc;
  }

  public void submitState(ZkNodeProps stateMessage) {
    // Don't allow publish of state we last published if not DOWNNODE?
    try {
      if (stateMessage != TERMINATE_OP) {
        String operation = stateMessage.getStr(OPERATION);
        if (operation.equals("state")) {
          String core = stateMessage.getStr(ZkStateReader.CORE_NAME_PROP);
          String state = stateMessage.getStr(ZkStateReader.STATE_PROP);
          String collection = stateMessage.getStr(ZkStateReader.COLLECTION_PROP);

          DocCollection coll = zkStateReader.getClusterState().getCollectionOrNull(collection);
          if (coll != null) {
            Replica replica = coll.getReplica(core);
            String lastState = stateCache.get(core);
            if (collection != null && replica != null && !state.equals(Replica.State.ACTIVE) && state.equals(lastState) && replica.getState().toString().equals(state)) {
              log.info("Skipping publish state as {} for {}, because it was the last state published", state, core);
              return;
            }
          }
          if (core == null || state == null) {
            log.error("Nulls in published state");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Nulls in published state " + stateMessage);
          }

          stateCache.put(core, state);
        } else if (operation.equalsIgnoreCase(OverseerAction.DOWNNODE.toLower())) {
          // set all statecache entries for replica to a state

          Collection<String> coreNames = cc.getAllCoreNames();
          for (String core : coreNames) {
            stateCache.put(core, Replica.State.getShortState(Replica.State.DOWN));
          }

        } else if (operation.equalsIgnoreCase(OverseerAction.RECOVERYNODE.toLower())) {
          // set all statecache entries for replica to a state

          Collection<String> coreNames = cc.getAllCoreNames();
          for (String core : coreNames) {
            stateCache.put(core, Replica.State.getShortState(Replica.State.RECOVERING));
          }

        } else {
          throw new IllegalArgumentException(stateMessage.toString());
        }
      }

      workQueue.offer(stateMessage);
    } catch (Exception e) {
      log.error("Exception trying to publish state message={}", stateMessage, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void clearStatCache(String core) {
    stateCache.remove(core);
  }

  public void start() {
    this.worker = new Worker();
    workerFuture = ParWork.getRootSharedExecutor().submit(this.worker);
  }

  public void close() {
    this.terminated = true;
    try {
      workerFuture.cancel(false);
    } catch (Exception e) {
      log.error("Exception waiting for close", e);
    }
  }
}
