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

import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StatePublisher implements Closeable {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String,String> stateCache = new ConcurrentHashMap<>(32, 0.75f, 4);

  public static class NoOpMessage extends ZkNodeProps {
  }

  public static final NoOpMessage TERMINATE_OP = new NoOpMessage();

  private final BlockingArrayQueue<ZkNodeProps> workQueue = new BlockingArrayQueue<>(30, 10);
  private final ZkDistributedQueue overseerJobQueue;
  private volatile Worker worker;
  private volatile Future<?> workerFuture;

  private volatile boolean terminated;
  private class Worker implements Runnable {

    Worker() {

    }

    @Override
    public void run() {
      while (!terminated) {
        ZkNodeProps message = null;
        ZkNodeProps bulkMessage = new ZkNodeProps();
        bulkMessage.getProperties().put("operation", "state");
        try {
          try {
            message = workQueue.poll(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {

          }
          if (message != null) {
            if (log.isDebugEnabled()) log.debug("Got state message " + message);
            if (message == TERMINATE_OP) {
              terminated = true;
              message = null;
            } else {
              bulkMessage(message, bulkMessage);
            }

            while (message != null) {
              try {
                message = workQueue.poll(30, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
              return;
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

        } catch (Exception e) {
          log.error("Exception in StatePublisher run loop", e);
          return;
        }
      }
    }

    private void bulkMessage(ZkNodeProps zkNodeProps, ZkNodeProps bulkMessage) throws KeeperException, InterruptedException {
      if (zkNodeProps.getStr("operation").equals("downnode")) {
        bulkMessage.getProperties().put("downnode", zkNodeProps.getStr(ZkStateReader.NODE_NAME_PROP));
      } else {
        String collection = zkNodeProps.getStr(ZkStateReader.COLLECTION_PROP);
        String core = zkNodeProps.getStr(ZkStateReader.CORE_NAME_PROP);
        String state = zkNodeProps.getStr(ZkStateReader.STATE_PROP);

        if (collection == null || core == null || state == null) {
          log.error("Bad state found for publish! {} {}", zkNodeProps, bulkMessage);
          return;
        }

        bulkMessage.getProperties().put(core, collection + "," + state);
      }
    }

    private void processMessage(ZkNodeProps message) throws KeeperException, InterruptedException {
      // do it in a separate thread so that we can be stopped by interrupt without screwing up the ZooKeeper client
      ParWork.getRootSharedExecutor().invokeAll(Collections.singletonList(() -> {
        overseerJobQueue.offer(Utils.toJSON(message));
        return null;
      }));
    }
  }

  public StatePublisher(ZkDistributedQueue overseerJobQueue) {
    this.overseerJobQueue = overseerJobQueue;
  }

  public void submitState(ZkNodeProps stateMessage) {
    // Don't allow publish of state we last published if not DOWNNODE
    if (stateMessage != TERMINATE_OP) {
      String operation = stateMessage.getStr("operation");
      if (operation.equals("state")) {
        String core = stateMessage.getStr(ZkStateReader.CORE_NAME_PROP);
        String state = stateMessage.getStr(ZkStateReader.STATE_PROP);


        String lastState = stateCache.get(core);
        // nocommit
//        if (state.equals(lastState) && !Replica.State.ACTIVE.toString().toLowerCase(Locale.ROOT).equals(state)) {
//          log.info("Skipping publish state as {} for {}, because it was the last state published", state, core);
//          // nocommit
//          return;
//        }
        if (core == null || state == null) {
          log.error("Nulls in published state");
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Nulls in published state " + stateMessage);
        }

        stateCache.put(core, state);
      } else if (operation.equalsIgnoreCase("downnode")) {
        // nocommit - set all statecache entries for replica to DOWN

      } else {
        throw new IllegalArgumentException(stateMessage.toString());
      }
    }

    workQueue.offer(stateMessage);
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
      workerFuture.cancel(true);
    } catch (Exception e) {
      log.error("Exception waiting for close", e);
    }
  }
}
