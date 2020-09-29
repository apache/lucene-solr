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
package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.NotEmptyException;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.RedactionUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only snapshot of another {@link DistribStateManager}
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class SnapshotDistribStateManager implements DistribStateManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  LinkedHashMap<String, VersionedData> dataMap = new LinkedHashMap<>();

  /**
   * Populate this instance from another {@link DistribStateManager} instance.
   * @param other another instance
   * @param config optional {@link AutoScalingConfig}, which will overwrite any existing config.
   */
  public SnapshotDistribStateManager(DistribStateManager other, AutoScalingConfig config) throws Exception {
    List<String> tree = other.listTree("/");
    if (log.isDebugEnabled()) {
      log.debug("- copying {} resources from {}", tree.size(), other.getClass().getSimpleName());
    }
    for (String path : tree) {
      dataMap.put(path, other.getData(path));
    }
    if (config != null) { // overwrite existing
      VersionedData vd = new VersionedData(config.getZkVersion(), Utils.toJSON(config), CreateMode.PERSISTENT, "0");
      dataMap.put(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, vd);
    }
  }

  /**
   * Populate this instance from a previously generated snapshot.
   * @param snapshot previous snapshot created using this class.
   */
  public SnapshotDistribStateManager(Map<String, Object> snapshot) {
    this(snapshot, null);
  }
  /**
   * Populate this instance from a previously generated snapshot.
   * @param snapshot previous snapshot created using this class.
   * @param config optional config to override the one from snapshot, may be null
   */
  public SnapshotDistribStateManager(Map<String, Object> snapshot, AutoScalingConfig config) {
    snapshot.forEach((path, value) -> {
      @SuppressWarnings({"unchecked"})
      Map<String, Object> map = (Map<String, Object>)value;
      Number version = (Number)map.getOrDefault("version", 0);
      String owner = (String)map.get("owner");
      String modeStr = (String)map.getOrDefault("mode", CreateMode.PERSISTENT.toString());
      CreateMode mode = CreateMode.valueOf(modeStr);
      byte[] bytes = null;
      if (map.containsKey("data")) {
        bytes = Base64.base64ToByteArray((String)map.get("data"));
      }
      dataMap.put(path, new VersionedData(version.intValue(), bytes, mode, owner));
    });
    if (config != null) { // overwrite existing
      VersionedData vd = new VersionedData(config.getZkVersion(), Utils.toJSON(config), CreateMode.PERSISTENT, "0");
      dataMap.put(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, vd);
    }
    if (log.isDebugEnabled()) {
      log.debug("- loaded snapshot of {} resources", dataMap.size());
    }
  }

  // content of these nodes is a UTF-8 String and it needs to be redacted
  private static final Set<Pattern> REDACTED = new HashSet<>();
  static {
    REDACTED.add(Pattern.compile("/aliases\\.json"));
    REDACTED.add(Pattern.compile("/autoscaling\\.json"));
    REDACTED.add(Pattern.compile("/clusterstate\\.json"));
    REDACTED.add(Pattern.compile("/collections/.*?/state\\.json"));
    REDACTED.add(Pattern.compile("/collections/.*?/leaders/shard.*?/leader"));
    REDACTED.add(Pattern.compile("/overseer_elect/leader"));
  }
  /**
   * Create a snapshot of all content in this instance.
   */
  public Map<String, Object> getSnapshot(RedactionUtils.RedactionContext ctx) {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    dataMap.forEach((path, vd) -> {
      Map<String, Object> data = new HashMap<>();
      if (vd.getData() != null && ctx != null && REDACTED.stream().anyMatch(p -> p.matcher(path).matches())) {
        String str = new String(vd.getData(), Charset.forName("UTF-8"));
        str = RedactionUtils.redactNames(ctx.getRedactions(), str);
        vd = new VersionedData(vd.getVersion(), str.getBytes(Charset.forName("UTF-8")), vd.getMode(), vd.getOwner());
      }
      vd.toMap(data);
      snapshot.put(path, data);
    });
    return snapshot;
  }

  @Override
  public boolean hasData(String path) throws IOException, KeeperException, InterruptedException {
    return dataMap.containsKey(path);
  }

  @Override
  public List<String> listData(String path) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    return listData(path, null);
  }

  @Override
  public List<String> listTree(String path) {
    return dataMap.keySet().stream()
        .filter(p -> p.startsWith(path))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> listData(String path, Watcher watcher) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    final String prefix = path + "/";
    return dataMap.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .map(e -> {
          String suffix = e.getKey().substring(prefix.length());
          int idx = suffix.indexOf('/');
          if (idx == -1) {
            return suffix;
          } else {
            return suffix.substring(0, idx);
          }
        })
        .collect(Collectors.toList());
  }

  @Override
  public VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    if (!dataMap.containsKey(path)) {
      throw new NoSuchElementException(path);
    }
    return dataMap.get(path);
  }

  @Override
  public void makePath(String path) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {
    throw new UnsupportedOperationException("makePath");
  }

  @Override
  public void makePath(String path, byte[] data, CreateMode createMode, boolean failOnExists) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {
    throw new UnsupportedOperationException("makePath");
  }

  @Override
  public String createData(String path, byte[] data, CreateMode mode) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {
    throw new UnsupportedOperationException("createData");
  }

  @Override
  public void removeData(String path, int version) throws NoSuchElementException, IOException, NotEmptyException, KeeperException, InterruptedException, BadVersionException {
    throw new UnsupportedOperationException("removeData");
  }

  @Override
  public void setData(String path, byte[] data, int version) throws BadVersionException, NoSuchElementException, IOException, KeeperException, InterruptedException {
    throw new UnsupportedOperationException("setData");
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws BadVersionException, NoSuchElementException, AlreadyExistsException, IOException, KeeperException, InterruptedException {
    throw new UnsupportedOperationException("multi");
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws InterruptedException, IOException {
    VersionedData vd = dataMap.get(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH);
    Map<String, Object> map = new HashMap<>();
    if (vd != null && vd.getData() != null && vd.getData().length > 0) {
      map = (Map<String, Object>) Utils.fromJSON(vd.getData());
      map.put(AutoScalingParams.ZK_VERSION, vd.getVersion());
    }
    return new AutoScalingConfig(map);
  }

  @Override
  public void close() throws IOException {

  }
}
