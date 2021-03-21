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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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
  static final String PREFIX = "qn-";
  public static final NoOpMessage TERMINATE_OP = new NoOpMessage();
  public static final ConcurrentHashMap TERMINATE_OP_MAP = new ConcurrentHashMap();

  private final ArrayBlockingQueue<ConcurrentHashMap> workQueue = new ArrayBlockingQueue<>(1024, true);
  private final ZkDistributedQueue overseerJobQueue;
  private volatile Worker worker;
  private volatile Future<?> workerFuture;

  private volatile boolean terminated;
  private class Worker implements Runnable {

    public static final int POLL_TIME_ON_PUBLISH_NODE = 1;
    public static final int POLL_TIME = 5;

    Worker() {

    }

    @Override
    public void run() {

      while (!terminated) {
        if (!zkStateReader.getZkClient().isAlive()) {
          try {
            zkStateReader.getZkClient().getConnectionManager().waitForConnected(5000);
          } catch (AlreadyClosedException e) {
            log.warn("Hit already closed exception while waiting for zkclient to reconnect");
            return;
          } catch (Exception e) {
            continue;
          }
        }
        ConcurrentHashMap message = null;
        ConcurrentHashMap bulkMessage = new ConcurrentHashMap();
        bulkMessage.put(OPERATION, "state");
        int pollTime = 250;
        try {
          try {
            log.debug("State publisher will poll for 5 seconds");
            message = workQueue.poll(5000, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
            log.warn("state publisher hit exception polling", e);
          }
          if (message != null) {
            log.debug("Got state message " + message);

            if (message == TERMINATE_OP_MAP) {
              log.debug("State publish is terminated");
              terminated = true;
              pollTime = 1;
            } else {
              if (bulkMessage(message, bulkMessage)) {
                pollTime = POLL_TIME_ON_PUBLISH_NODE;
              } else {
                pollTime = POLL_TIME;
              }
            }

            while (true) {
              try {
                log.debug("State publisher will poll for {} ms", pollTime);
                message = workQueue.poll(pollTime, TimeUnit.MILLISECONDS);
              } catch (Exception e) {
                log.warn("state publisher hit exception polling", e);
              }
              if (message != null) {
                if (log.isDebugEnabled()) log.debug("Got state message " + message);
                if (message == TERMINATE_OP_MAP) {
                  terminated = true;
                  pollTime = 1;
                } else {
                  if (bulkMessage(message, bulkMessage)) {
                    pollTime = POLL_TIME_ON_PUBLISH_NODE;
                  } else {
                    pollTime = POLL_TIME;
                  }
                }
              } else {
                break;
              }
            }
          }

          if (bulkMessage.size() > 1) {
            processMessage(bulkMessage);
          } else {
            log.debug("No messages to publish, loop");
          }

          if (terminated) {
            log.info("State publisher has terminated");
            break;
          }
        } catch (Exception e) {
          log.error("Exception in StatePublisher run loop", e);
        }
      }
    }

    private boolean bulkMessage(ConcurrentHashMap zkNodeProps, ConcurrentHashMap bulkMessage) {
      if (OverseerAction.get((String) zkNodeProps.get(OPERATION)) == OverseerAction.DOWNNODE) {
        String nodeName = (String) zkNodeProps.get(ZkStateReader.NODE_NAME_PROP);
        //clearStatesForNode(bulkMessage, nodeName);
        bulkMessage.put(OverseerAction.DOWNNODE.toLower(), nodeName);
        log.debug("bulk state publish down node, props={} result={}", zkNodeProps, bulkMessage);
        return true;
      } else if (OverseerAction.get((String) zkNodeProps.get(OPERATION)) == OverseerAction.RECOVERYNODE) {
        log.debug("bulk state publish recovery node, props={} result={}", zkNodeProps, bulkMessage);
        String nodeName = (String) zkNodeProps.get(ZkStateReader.NODE_NAME_PROP);
       // clearStatesForNode(bulkMessage, nodeName);
        bulkMessage.put(OverseerAction.RECOVERYNODE.toLower(), nodeName);
        log.debug("bulk state publish recovery node, props={} result={}" , zkNodeProps, bulkMessage);
        return true;
      } else {
        //String collection = zkNodeProps.getStr(ZkStateReader.COLLECTION_PROP);
        String core = (String) zkNodeProps.get(ZkStateReader.CORE_NAME_PROP);
        String id = (String) zkNodeProps.get("id");
        String state = (String) zkNodeProps.get(ZkStateReader.STATE_PROP);

        String line = Replica.State.getShortState(Replica.State.valueOf(state.toUpperCase(Locale.ROOT)));
        if (log.isDebugEnabled()) log.debug("bulk publish core={} id={} state={} line={}", core, id, state, line);
        bulkMessage.put(id, line);
//        if (state.equals(Replica.State.RECOVERING.toString())) {
//          return true;
//        }
      }
      return false;
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

    private void processMessage(ConcurrentHashMap message) throws KeeperException, InterruptedException {
      log.info("Send state updates to Overseer {}", message);
      byte[] updates = Utils.toJSON(message);

      zkStateReader.getZkClient().create("/overseer/queue" + "/" + PREFIX, updates, CreateMode.PERSISTENT_SEQUENTIAL, (rc, path, ctx, name, stat) -> {
        if (rc != 0) {
          log.error("got zk error deleting path {} {}", path, rc);
          KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path);
          log.error("Exception publish state messages path=" + path, e);
          workQueue.offer(message);
        }
      });
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

          DocCollection coll = zkStateReader.getClusterState().getCollectionOrNull(collection);

          if (coll == null) {
            zkStateReader.waitForState(collection, 5, TimeUnit.SECONDS, (liveNodes, collectionState) -> collectionState != null);
          }

          if (coll != null) {
            Replica replica = coll.getReplica(core);
            if (replica != null) {
              id = replica.getId();
            } else {
              id = stateMessage.getStr("id");
            }

            CacheEntry lastState = stateCache.get(id);
            //&& (System.currentTimeMillis() - lastState.time < 1000) &&
            // TODO: needs work
//            if (replica != null && replica.getType() == Replica.Type.PULL && lastState != null && state.equals(lastState.state) && (System.currentTimeMillis() - lastState.time < 10000)) {
//              log.info("Skipping publish state as {} for {}, because it was the last state published", state, core);
//              return;
//            }
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
          log.error("illegal state message {}", stateMessage.toString());
          throw new IllegalArgumentException(stateMessage.toString());
        }
      }

      if (stateMessage == TERMINATE_OP) {
        workQueue.offer(TERMINATE_OP_MAP);
      } else {
        workQueue.offer(new ConcurrentHashMap(stateMessage.getProperties()));
      }
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
    try {
      workerFuture.get();
    } catch (Exception e) {
      log.error("Exception waiting for close", e);
    }
  }
}
