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
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StatePublisher implements Closeable {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

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
          message = workQueue.poll(5, TimeUnit.SECONDS);
          if (message != null) {
            log.info("Got state message " + message);
            if (message == TERMINATE_OP) {
              return;
            }

            bulkMessage(message, bulkMessage);

            while (message != null) {
              message = workQueue.poll(0, TimeUnit.SECONDS);
              log.info("Got state message " + message);
              if (message != null) {
                if (message == TERMINATE_OP) {
                  return;
                }
                bulkMessage(message, bulkMessage);
              }
            }
            processMessage(bulkMessage);
          }

        } catch (InterruptedException e) {
          return;
        } catch (Exception e) {
          log.error("Exception in StatePublisher run loop", e);
          return;
        }
      }
    }

    private void bulkMessage(ZkNodeProps zkNodeProps, ZkNodeProps bulkMessage) throws KeeperException, InterruptedException {
      String collection = zkNodeProps.getStr(ZkStateReader.COLLECTION_PROP);
      String core = zkNodeProps.getStr(ZkStateReader.CORE_NAME_PROP);
      String state = zkNodeProps.getStr(ZkStateReader.STATE_PROP);

      bulkMessage.getProperties().put(core, collection + "," + state);
    }

    private void processMessage(ZkNodeProps message) throws KeeperException, InterruptedException {
      overseerJobQueue.offer(Utils.toJSON(message));
    }
  }

  public StatePublisher(ZkDistributedQueue overseerJobQueue) {
    this.overseerJobQueue = overseerJobQueue;
  }

  public void submitState(ZkNodeProps stateMessage) {
    workQueue.offer(stateMessage);
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
