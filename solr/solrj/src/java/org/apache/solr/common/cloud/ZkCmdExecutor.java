package org.apache.solr.common.cloud;

/**
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

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;


public class ZkCmdExecutor {
  private static final Logger LOG = Logger.getLogger(ZkCmdExecutor.class);
  
  protected final SolrZkClient zkClient;
  private long retryDelay = 1000L;
  private int retryCount = 15;
  private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  
  public ZkCmdExecutor(SolrZkClient solrZkClient) {
    this.zkClient = solrZkClient;
  }
  
  /**
   * return the acl its using
   * 
   * @return the acl.
   */
  public List<ACL> getAcl() {
    return acl;
  }
  
  /**
   * set the acl
   * 
   * @param acl
   *          the acl to set to
   */
  public void setAcl(List<ACL> acl) {
    this.acl = acl;
  }
  
  /**
   * get the retry delay in milliseconds
   * 
   * @return the retry delay
   */
  public long getRetryDelay() {
    return retryDelay;
  }
  
  /**
   * Sets the time waited between retry delays
   * 
   * @param retryDelay
   *          the retry delay
   */
  public void setRetryDelay(long retryDelay) {
    this.retryDelay = retryDelay;
  }
  
  /**
   * Perform the given operation, retrying if the connection fails
   * 
   * @return object. it needs to be cast to the callee's expected return type.
   * @throws IOException 
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
        retryDelay(i);
      }
    }
    throw exception;
  }
  
  /**
   * Ensures that the given path exists with no data, the current ACL and no
   * flags
   * 
   * @param path
   * @throws IOException 
   */
  protected void ensurePathExists(String path) {
    ensureExists(path, null, acl, CreateMode.PERSISTENT);
  }
  
  /**
   * Ensures that the given path exists with the given data, ACL and flags
   * 
   * @param path
   * @param acl
   * @param flags
   * @throws IOException 
   */
  protected void ensureExists(final String path, final byte[] data,
      final List<ACL> acl, final CreateMode flags) {
    try {
      retryOperation(new ZkOperation() {
        public Object execute() throws KeeperException, InterruptedException {
          if (zkClient.exists(path)) {
            return true;
          }
          zkClient.create(path, data, acl, flags);
          return true;
        }
      });
    } catch (KeeperException e) {
      LOG.warn("", e);
    } catch (InterruptedException e) {
      LOG.warn("", e);
    }
  }
  
  /**
   * Performs a retry delay if this is not the first attempt
   * 
   * @param attemptCount
   *          the number of the attempts performed so far
   */
  protected void retryDelay(int attemptCount) {
    if (attemptCount > 0) {
      try {
        Thread.sleep(attemptCount * retryDelay);
      } catch (InterruptedException e) {
        LOG.debug("Failed to sleep: " + e, e);
      }
    }
  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }
}
