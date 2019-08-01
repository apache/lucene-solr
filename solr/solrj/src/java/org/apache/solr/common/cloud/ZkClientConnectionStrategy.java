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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkCredentialsProvider.ZkCredentials;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class ZkClientConnectionStrategy {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile ZkCredentialsProvider zkCredentialsToAddAutomatically;
  private volatile boolean zkCredentialsToAddAutomaticallyUsed;

  private List<DisconnectedListener> disconnectedListeners = new ArrayList<>();
  private List<ConnectedListener> connectedListeners = new ArrayList<>();

  public abstract void connect(String zkServerAddress, int zkClientTimeout, Watcher watcher, ZkUpdate updater) throws IOException, InterruptedException, TimeoutException;
  public abstract void reconnect(String serverAddress, int zkClientTimeout, Watcher watcher, ZkUpdate updater) throws IOException, InterruptedException, TimeoutException;

  public ZkClientConnectionStrategy() {
    zkCredentialsToAddAutomaticallyUsed = false;
  }

  public synchronized void disconnected() {
    for (DisconnectedListener listener : disconnectedListeners) {
      try {
        listener.disconnected();
      } catch (Exception e) {
        SolrException.log(log, "", e);
      }
    }
  }

  public synchronized void connected() {
    for (ConnectedListener listener : connectedListeners) {
      try {
        listener.connected();
      } catch (Exception e) {
        SolrException.log(log, "", e);
      }
    }
  }

  public interface DisconnectedListener {
    void disconnected();
  }

  public interface ConnectedListener {
    void connected();
  }


  public synchronized void addDisconnectedListener(DisconnectedListener listener) {
    disconnectedListeners.add(listener);
  }

  public synchronized void removeDisconnectedListener(DisconnectedListener listener) {
    disconnectedListeners.remove(listener);
  }

  public synchronized void addConnectedListener(ConnectedListener listener) {
    connectedListeners.add(listener);
  }

  public interface ZkUpdate {
    void update(SolrZooKeeper zooKeeper) throws InterruptedException, TimeoutException, IOException;
  }

  public void setZkCredentialsToAddAutomatically(ZkCredentialsProvider zkCredentialsToAddAutomatically) {
    if (zkCredentialsToAddAutomaticallyUsed || (zkCredentialsToAddAutomatically == null))
      throw new RuntimeException("Cannot change zkCredentialsToAddAutomatically after it has been (connect or reconnect was called) used or to null");
    this.zkCredentialsToAddAutomatically = zkCredentialsToAddAutomatically;
  }

  public boolean hasZkCredentialsToAddAutomatically() {
    return zkCredentialsToAddAutomatically != null;
  }

  public ZkCredentialsProvider getZkCredentialsToAddAutomatically() { return zkCredentialsToAddAutomatically; }

  protected SolrZooKeeper createSolrZooKeeper(final String serverAddress, final int zkClientTimeout,
      final Watcher watcher) throws IOException {
    SolrZooKeeper result = new SolrZooKeeper(serverAddress, zkClientTimeout, watcher);

    zkCredentialsToAddAutomaticallyUsed = true;
    for (ZkCredentials zkCredentials : zkCredentialsToAddAutomatically.getCredentials()) {
      result.addAuthInfo(zkCredentials.getScheme(), zkCredentials.getAuth());
    }

    return result;
  }

}
