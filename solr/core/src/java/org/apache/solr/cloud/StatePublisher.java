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
import org.apache.solr.core.CoreDescriptor;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StatePublisher implements Closeable {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  public static final String OPERATION = "op";


  private static class CacheEntry {
    String state;
    long time;
  }

  private final Map<String,CacheEntry> stateCache = new ConcurrentHashMap<>(32, 0.75f, 6);
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

      while (!terminated && !zkStateReader.getZkClient().isClosed()) {
        if (!zkStateReader.getZkClient().isConnected()) {
          try {
            zkStateReader.getZkClient().getConnectionManager().waitForConnected(5000);
          } catch (TimeoutException e) {
            continue;
          } catch (InterruptedException e) {
            log.error("publisher interrupted", e);
          }
          continue;
        }

        ZkNodeProps message = null;
        ZkNodeProps bulkMessage = new ZkNodeProps();
        bulkMessage.getProperties().put(OPERATION, "state");
        try {
          try {
            message = workQueue.poll(1000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {

          }
          if (message != null) {
            log.debug("Got state message " + message);
            if (message == TERMINATE_OP) {
              log.debug("State publish is terminated");
              terminated = true;
              return;
            } else {
              bulkMessage(message, bulkMessage);
            }

            while (!terminated) {
              try {
                message = workQueue.poll(100, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                log.warn("state publisher interrupted", e);
                return;
              }
              if (message != null) {
                if (log.isDebugEnabled()) log.debug("Got state message " + message);
                if (message == TERMINATE_OP) {
                  terminated = true;
                } else {
                  bulkMessage(message, bulkMessage);
                }
              } else {
                break;
              }
            }
          }

          if (bulkMessage.getProperties().size() > 1) {
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

    private void bulkMessage(ZkNodeProps zkNodeProps, ZkNodeProps bulkMessage) {
      if (log.isDebugEnabled()) log.debug("Bulk state zkNodeProps={} bulkMessage={}", zkNodeProps, bulkMessage);
      if (OverseerAction.get(zkNodeProps.getStr(OPERATION)) == OverseerAction.DOWNNODE) {
        String nodeName = zkNodeProps.getStr(ZkStateReader.NODE_NAME_PROP);
        //clearStatesForNode(bulkMessage, nodeName);
        bulkMessage.getProperties().put(OverseerAction.DOWNNODE.toLower(), nodeName);
        log.debug("bulk state publish down node, props={} result={}", zkNodeProps, bulkMessage);

      } else if (OverseerAction.get(zkNodeProps.getStr(OPERATION)) == OverseerAction.RECOVERYNODE) {
        log.debug("bulk state publish recovery node, props={} result={}", zkNodeProps, bulkMessage);
        String nodeName = zkNodeProps.getStr(ZkStateReader.NODE_NAME_PROP);
       // clearStatesForNode(bulkMessage, nodeName);
        bulkMessage.getProperties().put(OverseerAction.RECOVERYNODE.toLower(), nodeName);
        log.debug("bulk state publish recovery node, props={} result={}" , zkNodeProps, bulkMessage);
      } else {
        //String collection = zkNodeProps.getStr(ZkStateReader.COLLECTION_PROP);
        String core = zkNodeProps.getStr(ZkStateReader.CORE_NAME_PROP);
        String id = zkNodeProps.getStr("id");
        String state = zkNodeProps.getStr(ZkStateReader.STATE_PROP);

        String line = Replica.State.getShortState(Replica.State.valueOf(state.toUpperCase(Locale.ROOT)));
        if (log.isDebugEnabled()) log.debug("bulk publish core={} id={} state={} line={}", core, id, state, line);
        bulkMessage.getProperties().put(id, line);
      }
    }

    private void clearStatesForNode(ZkNodeProps bulkMessage, String nodeName) {
      Set<String> removeIds = new HashSet<>();
      Set<String> ids = bulkMessage.getProperties().keySet();
      for (String id : ids) {
        if (id.equals(OverseerAction.DOWNNODE.toLower()) || id.equals(OverseerAction.RECOVERYNODE.toLower())) {
          continue;
        }
        Collection<DocCollection> collections = zkStateReader.getClusterState().getCollectionsMap().values();
        for (DocCollection collection : collections) {
          Replica replica = collection.getReplicaById(id);
          if (replica != null) {
            if (replica.getNodeName().equals(nodeName)) {
              removeIds.add(id);
            }
          }
        }

      }
      for (String id : removeIds) {
        bulkMessage.getProperties().remove(id);
      }
    }

    private void processMessage(ZkNodeProps message) throws KeeperException, InterruptedException {
      log.debug("Send state updates to Overseer {}", message);
      byte[] updates = Utils.toJSON(message);
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
        String id = null;
        if (operation.equals("state")) {
          String core = stateMessage.getStr(ZkStateReader.CORE_NAME_PROP);
          String collection = stateMessage.getStr(ZkStateReader.COLLECTION_PROP);
          String state = stateMessage.getStr(ZkStateReader.STATE_PROP);

          log.debug("submit state for publishing core={} state={}", core, state);

          if (core == null || state == null) {
            log.error("Nulls in published state");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Nulls in published state " + stateMessage);
          }

//          if ((state.equals(UpdateLog.State.ACTIVE.toString().toLowerCase(Locale.ROOT)) || state.equals("leader")) && cc.isCoreLoading(core)) {
//            cc.waitForLoadingCore(core, 10000);
//          }

          DocCollection coll = zkStateReader.getClusterState().getCollectionOrNull(collection);
          if (coll != null) {
            Replica replica = coll.getReplica(core);
            if (replica != null) {
              id = replica.getId();
            } else {
              id = stateMessage.getStr("id");
            }

            CacheEntry lastState = stateCache.get(id);
            //&& (System.currentTimeMillis() - lastState.time < 1000) &&
            if (!state.equals("leader") && collection != null && lastState != null && replica != null && !state.equals(lastState.state) && replica.getState().toString().equals(state)) {
              log.info("Skipping publish state as {} for {}, because it was the last state published", state, core);
              return;
            }
          }

          if (id == null) {
            id = stateMessage.getStr("id");
          }

          stateMessage.getProperties().put("id", id);
          CacheEntry cacheEntry = new CacheEntry();
          cacheEntry.time = System.currentTimeMillis();
          cacheEntry.state = state;
          stateCache.put(id, cacheEntry);
        } else if (operation.equalsIgnoreCase(OverseerAction.DOWNNODE.toLower())) {
          // set all statecache entries for replica to a state

          Collection<CoreDescriptor> cds = cc.getCoreDescriptors();
          for (CoreDescriptor cd : cds) {
            DocCollection doc = zkStateReader.getClusterState().getCollectionOrNull(cd.getCollectionName());
            Replica replica = null;
            if (doc != null) {
              replica = doc.getReplica(cd.getName());

              if (replica != null) {
                CacheEntry cacheEntry = new CacheEntry();
                cacheEntry.time = System.currentTimeMillis();
                cacheEntry.state = Replica.State.getShortState(Replica.State.DOWN);
                stateCache.put(replica.getId(), cacheEntry);
              }
            }
          }

        } else if (operation.equalsIgnoreCase(OverseerAction.RECOVERYNODE.toLower())) {
          // set all statecache entries for replica to a state

          Collection<CoreDescriptor> cds = cc.getCoreDescriptors();
          for (CoreDescriptor cd : cds) {
            DocCollection doc = zkStateReader.getClusterState().getCollectionOrNull(cd.getCollectionName());
            Replica replica = null;
            if (doc != null) {
              replica = doc.getReplica(cd.getName());

              if (replica != null) {
                CacheEntry cacheEntry = new CacheEntry();
                cacheEntry.time = System.currentTimeMillis();
                cacheEntry.state = Replica.State.getShortState(Replica.State.RECOVERING);
                stateCache.put(replica.getId(), cacheEntry);
              }
            }
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

  public void clearStatCache() {
    stateCache.clear();
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
