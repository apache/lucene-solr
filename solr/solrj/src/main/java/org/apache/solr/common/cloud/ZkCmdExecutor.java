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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;


public class ZkCmdExecutor {
  private long retryDelay = 1500L; // 1 second would match timeout, so 500 ms over for padding
  private int retryCount;
  private double timeouts;
  
  /**
   * TODO: At this point, this should probably take a SolrZkClient in
   * its constructor.
   * 
   * @param timeoutms
   *          the client timeout for the ZooKeeper clients that will be used
   *          with this class.
   */
  public ZkCmdExecutor(int timeoutms) {
    timeouts = timeoutms / 1000.0;
    this.retryCount = Math.round(0.5f * ((float)Math.sqrt(8.0f * timeouts + 1.0f) - 1.0f)) + 1;
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
    for (int i = 0; i < retryCount; i++) {
      try {
        return (T) operation.execute();
      } catch (KeeperException.ConnectionLossException e) {
        if (exception == null) {
          exception = e;
        }
        if (Thread.currentThread().isInterrupted()) {
          Thread.currentThread().interrupt();
          throw new InterruptedException();
        }
        if (i != retryCount -1) {
          retryDelay(i);
        }
      }
    }
    throw exception;
  }
  
  public void ensureExists(String path, final SolrZkClient zkClient) throws KeeperException, InterruptedException {
    ensureExists(path, null, CreateMode.PERSISTENT, zkClient, 0);
  }
  
  
  public void ensureExists(String path, final byte[] data, final SolrZkClient zkClient) throws KeeperException, InterruptedException {
    ensureExists(path, data, CreateMode.PERSISTENT, zkClient, 0);
  }
  
  public void ensureExists(String path, final byte[] data, CreateMode createMode, final SolrZkClient zkClient) throws KeeperException, InterruptedException {
    ensureExists(path, data, createMode, zkClient, 0);
  }
  
  public void ensureExists(final String path, final byte[] data,
      CreateMode createMode, final SolrZkClient zkClient, int skipPathParts) throws KeeperException, InterruptedException {
    
    if (zkClient.exists(path, true)) {
      return;
    }
    try {
      zkClient.makePath(path, data, createMode, null, true, true, skipPathParts);
    } catch (NodeExistsException e) {
      // it's okay if another beats us creating the node
    }
    
  }
  
  /**
   * Performs a retry delay if this is not the first attempt
   * 
   * @param attemptCount
   *          the number of the attempts performed so far
   */
  protected void retryDelay(int attemptCount) throws InterruptedException {
    Thread.sleep((attemptCount + 1) * retryDelay);
  }

}
