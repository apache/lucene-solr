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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class DelegatingDistribStateManager implements DistribStateManager {
  private final DistribStateManager delegate;

  public DelegatingDistribStateManager(DistribStateManager delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean hasData(String path) throws IOException, KeeperException, InterruptedException {
    return delegate.hasData(path);
  }

  @Override
  public List<String> listData(String path) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    return delegate.listData(path);
  }

  @Override
  public List<String> listData(String path, Watcher watcher) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    return delegate.listData(path, watcher);
  }

  @Override
  public VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    return delegate.getData(path, watcher);
  }

  @Override
  public VersionedData getData(String path) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    return delegate.getData(path);
  }

  @Override
  public void makePath(String path) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {
    delegate.makePath(path);
  }

  @Override
  public void makePath(String path, byte[] data, CreateMode createMode, boolean failOnExists) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {
    delegate.makePath(path, data, createMode, failOnExists);
  }

  @Override
  public String createData(String path, byte[] data, CreateMode mode) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {
    return delegate.createData(path, data, mode);
  }

  @Override
  public void removeData(String path, int version) throws NoSuchElementException, NotEmptyException, IOException, BadVersionException, KeeperException, InterruptedException {
    delegate.removeData(path, version);
  }

  @Override
  public void setData(String path, byte[] data, int version) throws BadVersionException, NoSuchElementException, IOException, KeeperException, InterruptedException {
    delegate.setData(path, data, version);
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws BadVersionException, NoSuchElementException, AlreadyExistsException, IOException, KeeperException, InterruptedException {
    return delegate.multi(ops);
  }

  @Override
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws InterruptedException, IOException {
    return delegate.getAutoScalingConfig(watcher);
  }

  @Override
  public AutoScalingConfig getAutoScalingConfig() throws InterruptedException, IOException {
    return delegate.getAutoScalingConfig();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
