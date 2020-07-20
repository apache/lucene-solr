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
package org.apache.solr.common.cloud;

import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ConnectionManager.IsClosed;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class ZkCmdExecutor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private long retryDelay = 50L;
  private int retryCount;
  private long timeoutms;
  private IsClosed isClosed;
  
  public ZkCmdExecutor(int timeoutms) {
    this(timeoutms, null);
  }
  
  /**
   * TODO: At this point, this should probably take a SolrZkClient in
   * its constructor.
   * 
   * @param timeoutms
   *          the client timeout for the ZooKeeper clients that will be used
   *          with this class.
   */
  public ZkCmdExecutor(long timeoutms, IsClosed isClosed) {
    this.timeoutms = timeoutms;
    this.isClosed = isClosed;
  }
  
  public long getRetryDelay() {
    return retryDelay;
  }
  
  public void setRetryDelay(long retryDelay) {
    this.retryDelay = retryDelay;
  }
  

  /**
   * Perform the given operation, retrying if the connection fails
   */
  @SuppressWarnings("unchecked")
  public <T> T retryOperation(ZkOperation operation)
      throws KeeperException, InterruptedException {
    KeeperException exception = null;
    TimeOut timeout = new TimeOut(timeoutms, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    int tryCnt = 0;
    while (true) {
      try {

        if (timeout.hasTimedOut()) {
          throw new RuntimeException("Timed out attempting zk call");
        }

        return (T) operation.execute();
      } catch (KeeperException.ConnectionLossException e) {
        log.warn("ConnectionLost to ZK");
        if (exception == null) {
          exception = e;
        }
        if (Thread.currentThread().isInterrupted()) {
          Thread.currentThread().interrupt();
          throw new InterruptedException();
        }

        if (timeout.hasTimedOut()) {
          throw new RuntimeException("Timed out attempting zk call");
        }

        retryDelay(tryCnt);
      }
      tryCnt++;
    }
  }
  
  /**
   * Performs a retry delay if this is not the first attempt
   * 
   * @param attemptCount
   *          the number of the attempts performed so far
   */
  protected void retryDelay(int attemptCount) throws InterruptedException {
    long sleep = retryDelay;
    log.info("delaying for retry, attempt={} retryDelay={} sleep={} timeout={}", attemptCount, retryDelay, sleep, timeoutms);
    Thread.sleep(sleep);
  }

}
