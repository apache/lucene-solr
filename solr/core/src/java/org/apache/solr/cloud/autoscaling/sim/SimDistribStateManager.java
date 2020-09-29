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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jute.Record;
import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.NotEmptyException;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.util.IdUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulated {@link DistribStateManager} that keeps all data locally in a static structure. Instances of this
 * class are identified by their id in order to simulate the deletion of ephemeral nodes when {@link #close()} is
 * invoked.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class SimDistribStateManager implements DistribStateManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final class Node {
    ReentrantLock dataLock = new ReentrantLock();
    private int version = 0;
    private int seq = 0;
    private final CreateMode mode;
    // copyFrom needs to modify this
    private String owner;
    private final String path;
    private final String name;
    private final Node parent;
    private byte[] data = null;
    private Map<String, Node> children = new ConcurrentHashMap<>();
    Set<Watcher> dataWatches = ConcurrentHashMap.newKeySet();
    Set<Watcher> childrenWatches = ConcurrentHashMap.newKeySet();

    Node(Node parent, String name, String path, CreateMode mode, String owner) {
      this.parent = parent;
      this.name = name;
      this.path = path;
      this.mode = mode;
      this.owner = owner;
    }

    Node(Node parent, String name, String path, byte[] data, CreateMode mode, String owner) {
      this(parent, name, path, mode, owner);
      this.data = data;
    }

    public void clear() {
      dataLock.lock();
      try {
        children.clear();
        version = 0;
        seq = 0;
        dataWatches.clear();
        childrenWatches.clear();
        data = null;
      } finally {
        dataLock.unlock();
      }
    }

    public void setData(byte[] data, int version) throws BadVersionException, IOException {
      Set<Watcher> currentWatchers = new HashSet<>(dataWatches);
      dataLock.lock();
      try {
        if (version != -1 && version != this.version) {
          throw new BadVersionException(version, path);
        }
        if (data != null) {
          this.data = Arrays.copyOf(data, data.length);
        } else {
          this.data = null;
        }
        this.version++;
        dataWatches.clear();
      } finally {
        dataLock.unlock();
      }
      for (Watcher w : currentWatchers) {
        w.process(new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, path));
      }
    }

    public VersionedData getData(Watcher w) {
      dataLock.lock();
      try {
        VersionedData res = new VersionedData(version, data, mode, owner);
        if (w != null && !dataWatches.contains(w)) {
          dataWatches.add(w);
        }
        return res;
      } finally {
        dataLock.unlock();
      }
    }

    public void setChild(String name, Node child) {
      assert child.name.equals(name);
      Set<Watcher> currentWatchers = new HashSet<>(childrenWatches);
      dataLock.lock();
      try {
        children.put(name, child);
        childrenWatches.clear();
      } finally {
        dataLock.unlock();
      }
      for (Watcher w : currentWatchers) {
        w.process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, path));
      }
    }

    public void removeChild(String name, int version) throws NoSuchElementException, BadVersionException, IOException {
      Node n = children.get(name);
      if (n == null) {
        throw new NoSuchElementException(path + "/" + name);
      }
      if (version != -1 && version != n.version) {
        throw new BadVersionException(version, path);
      }
      children.remove(name);
      Set<Watcher> currentWatchers = new HashSet<>(childrenWatches);
      childrenWatches.clear();
      for (Watcher w : currentWatchers) {
        w.process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, path));
      }
      currentWatchers = new HashSet<>(n.dataWatches);
      n.dataWatches.clear();
      for (Watcher w : currentWatchers) {
        w.process(new WatchedEvent(Watcher.Event.EventType.NodeDeleted, Watcher.Event.KeeperState.SyncConnected, n.path));
      }
      // TODO: not sure if it's correct to recurse and fire watches???
      Set<String> kids = new HashSet<>(n.children.keySet());
      for (String kid : kids) {
        n.removeChild(kid, -1);
      }
    }

    public void removeEphemeralChildren(String id) throws NoSuchElementException, BadVersionException, IOException {
      Set<String> kids = new HashSet<>(children.keySet());
      for (String kid : kids) {
        Node n = children.get(kid);
        if (n == null) {
          continue;
        }
        if ((CreateMode.EPHEMERAL == n.mode || CreateMode.EPHEMERAL_SEQUENTIAL == n.mode) &&
            id.equals(n.owner)) {
          removeChild(n.name, -1);
        } else {
          n.removeEphemeralChildren(id);
        }
      }
    }

  }

  private final ReentrantLock multiLock = new ReentrantLock();

  public static Node createNewRootNode() {
    return new Node(null, "", "/", CreateMode.PERSISTENT, "0");
  }

  private final ExecutorService watchersPool;

  private final AtomicReference<ActionThrottle> throttleRef = new AtomicReference<>();
  private final AtomicReference<ActionError> errorRef = new AtomicReference<>();
  private final String id;
  private final Node root;

  private int juteMaxbuffer = 0xfffff;

  public SimDistribStateManager() {
    this(null);
  }

  /**
   * Construct new state manager that uses provided root node for storing data.
   * @param root if null then a new root node will be created.
   */
  public SimDistribStateManager(Node root) {
    this.id = IdUtils.timeRandomId();
    this.root = root != null ? root : createNewRootNode();
    watchersPool = ExecutorUtil.newMDCAwareFixedThreadPool(10, new SolrNamedThreadFactory("sim-watchers"));
    String bufferSize = System.getProperty("jute.maxbuffer", Integer.toString(0xffffff));
    juteMaxbuffer = Integer.parseInt(bufferSize);
  }

  /**
   * Copy all content from another DistribStateManager.
   * @param other another state manager.
   * @param failOnExists abort copy when one or more paths already exist (the state of this manager remains unchanged).
   */
  public void copyFrom(DistribStateManager other, boolean failOnExists) throws InterruptedException, IOException, KeeperException, AlreadyExistsException, BadVersionException {
    List<String> tree = other.listTree("/");
    if (log.isInfoEnabled()) {
      log.info("- copying {} resources...", tree.size());
    }
    // check if any node exists
    for (String path : tree) {
      if (hasData(path) && failOnExists) {
        throw new AlreadyExistsException(path);
      }
    }
    for (String path : tree) {
      VersionedData data = other.getData(path);
      if (hasData(path)) {
        setData(path, data.getData(), -1);
      } else {
        makePath(path, data.getData(), data.getMode(), failOnExists);
      }
      // hack: set the version and owner to be the same as the source
      Node n = traverse(path, false, CreateMode.PERSISTENT);
      n.version = data.getVersion();
      n.owner = data.getOwner();
    }
  }

  public SimDistribStateManager(ActionThrottle actionThrottle, ActionError actionError) {
    this(null, actionThrottle, actionError);
  }

  public SimDistribStateManager(Node root, ActionThrottle actionThrottle, ActionError actionError) {
    this(root);
    this.throttleRef.set(actionThrottle);
    this.errorRef.set(actionError);
  }

  private SimDistribStateManager(String id, ExecutorService watchersPool, Node root, ActionThrottle actionThrottle,
                                 ActionError actionError) {
    this.id = id;
    this.watchersPool = watchersPool;
    this.root = root;
    this.throttleRef.set(actionThrottle);
    this.errorRef.set(actionError);
  }

  /**
   * Create a copy of this instance using a specified ephemeral owner id. This is useful when performing
   * node operations that require using a specific id. Note: this instance should never be closed, it can
   * be just discarded after use.
   * @param id ephemeral owner id
   */
  public SimDistribStateManager withEphemeralId(String id) {
    return new SimDistribStateManager(id, watchersPool, root, throttleRef.get(), errorRef.get()) {
      @Override
      public void close() {
        throw new UnsupportedOperationException("this instance should never be closed - instead close the parent instance.");
      }
    };
  }

  /**
   * Get the root node of the tree used by this instance. It could be a static shared root node.
   */
  public Node getRoot() {
    return root;
  }

  /**
   * Clear this instance. All nodes, watchers and data is deleted.
   */
  public void clear() {
    root.clear();
  }

  private void throttleOrError(String path) throws IOException {
    ActionError err = errorRef.get();
    if (err != null && err.shouldFail(path)) {
      throw new IOException("Simulated error, path=" + path);
    }
    ActionThrottle throttle = throttleRef.get();
    if (throttle != null) {
      throttle.minimumWaitBetweenActions();
      throttle.markAttemptingAction();
    }
  }

  // this method should always be invoked under lock
  private Node traverse(String path, boolean create, CreateMode mode) throws IOException {
    if (path == null || path.isEmpty()) {
      return null;
    }
    throttleOrError(path);
    if (path.equals("/")) {
      return root;
    }
    if (path.charAt(0) == '/') {
      path = path.substring(1);
    }
    StringBuilder currentPath = new StringBuilder();
    String[] elements = path.split("/");
    Node parentNode = root;
    Node n = null;
    for (int i = 0; i < elements.length; i++) {
      String currentName = elements[i];
      currentPath.append('/');
      n = parentNode.children != null ? parentNode.children.get(currentName) : null;
      if (n == null) {
        if (create) {
          n = createNode(parentNode, mode, currentPath, currentName,null, true);
        } else {
          break;
        }
      } else {
        currentPath.append(currentName);
      }
      parentNode = n;
    }
    return n;
  }

  private Node createNode(Node parentNode, CreateMode mode, StringBuilder fullChildPath, String baseChildName, byte[] data, boolean attachToParent) throws IOException {
    String nodeName = baseChildName;
    if ((parentNode.mode == CreateMode.EPHEMERAL || parentNode.mode == CreateMode.EPHEMERAL_SEQUENTIAL) &&
        (mode == CreateMode.EPHEMERAL || mode == CreateMode.EPHEMERAL_SEQUENTIAL)) {
      throw new IOException("NoChildrenEphemerals for " + parentNode.path);
    }
    if (CreateMode.PERSISTENT_SEQUENTIAL == mode || CreateMode.EPHEMERAL_SEQUENTIAL == mode) {
      nodeName = nodeName + String.format(Locale.ROOT, "%010d", parentNode.seq);
      parentNode.seq++;
    }

    fullChildPath.append(nodeName);
    String owner = mode == CreateMode.EPHEMERAL || mode == CreateMode.EPHEMERAL_SEQUENTIAL ? id : "0";
    Node child = new Node(parentNode, nodeName, fullChildPath.toString(), data, mode, owner);

    if (attachToParent) {
      parentNode.setChild(nodeName, child);
    }
    return child;
  }

  @Override
  public void close() throws IOException {
    multiLock.lock();
    try {
      // remove all my ephemeral nodes
      root.removeEphemeralChildren(id);
    } catch (BadVersionException e) {
      // not happening
    } finally {
      multiLock.unlock();
    }

  }

  @Override
  public boolean hasData(String path) throws IOException {
    multiLock.lock();
    try {
      return traverse(path, false, CreateMode.PERSISTENT) != null;
    } finally {
      multiLock.unlock();
    }
  }

  @Override
  public List<String> listData(String path) throws NoSuchElementException, IOException {
    multiLock.lock();
    try {
      Node n = traverse(path, false, CreateMode.PERSISTENT);
      if (n == null) {
        throw new NoSuchElementException(path);
      }
      List<String> res = new ArrayList<>(n.children.keySet());
      Collections.sort(res);
      return res;
    } finally {
      multiLock.unlock();
    }
  }

  @Override
  public List<String> listData(String path, Watcher watcher) throws NoSuchElementException, IOException {
    Node n;
    List<String> res;
    multiLock.lock();
    try {
      n = traverse(path, false, CreateMode.PERSISTENT);
      if (n == null) {
        throw new NoSuchElementException(path);
      }
      res = new ArrayList<>(n.children.keySet());
      Collections.sort(res);
    } finally {
      multiLock.unlock();
    }
    if (watcher != null) {
      n.dataWatches.add(watcher);
      n.childrenWatches.add(watcher);
    }
    return res;
  }

  @Override
  public VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException {
    Node n = null;
    multiLock.lock();
    try {
      n = traverse(path, false, CreateMode.PERSISTENT);
      if (n == null) {
        throw new NoSuchElementException(path);
      }
    } finally {
      multiLock.unlock();
    }
    return n.getData(watcher);
  }

  @Override
  public void makePath(String path) throws IOException {
    multiLock.lock();
    try {
      traverse(path, true, CreateMode.PERSISTENT);
    } finally {
      multiLock.unlock();
    }
  }

  @Override
  public void makePath(String path, byte[] data, CreateMode createMode, boolean failOnExists) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {
    Node n = null;
    multiLock.lock();
    try {
      if (failOnExists && hasData(path)) {
        throw new AlreadyExistsException(path);
      }
      n = traverse(path, true, createMode);
    } finally {
      multiLock.unlock();
    }
    try {
      n.setData(data, -1);
    } catch (BadVersionException e) {
      throw new IOException("should not happen!", e);
    }
  }

  @Override
  public String createData(String path, byte[] data, CreateMode mode) throws AlreadyExistsException, NoSuchElementException, IOException {
    if ((CreateMode.EPHEMERAL == mode || CreateMode.PERSISTENT == mode) && hasData(path)) {
      throw new AlreadyExistsException(path);
    }

    String relPath = path.charAt(0) == '/' ? path.substring(1) : path;
    if (relPath.length() == 0) { //Trying to create root-node, return null.
      // TODO should trying to create a root node throw an exception since its always init'd in the ctor?
      return null;
    }

    String[] elements = relPath.split("/");
    StringBuilder parentStringBuilder = new StringBuilder();
    Node parentNode = null;
    if (elements.length == 1) { // Direct descendant of '/'.
      parentNode = getRoot();
    } else { // Indirect descendant of '/', lookup parent node
      for (int i = 0; i < elements.length - 1; i++) {
        parentStringBuilder.append('/');
        parentStringBuilder.append(elements[i]);
      }
      if (!hasData(parentStringBuilder.toString())) {
        throw new NoSuchElementException(parentStringBuilder.toString());
      }
      parentNode = traverse(parentStringBuilder.toString(), false, mode);
    }

    multiLock.lock();
    try {
      String nodeName = elements[elements.length-1];
      Node childNode = createNode(parentNode, mode, parentStringBuilder.append("/"), nodeName, data,false);
      parentNode.setChild(childNode.name, childNode);
      return childNode.path;
    } finally {
      multiLock.unlock();
    }

  }

  @Override
  public void removeData(String path, int version) throws NoSuchElementException, NotEmptyException, BadVersionException, IOException {
    multiLock.lock();
    Node parent;
    Node n;
    try {
      n = traverse(path, false, CreateMode.PERSISTENT);
      if (n == null) {
        throw new NoSuchElementException(path);
      }
      parent = n.parent;
      if (parent == null) {
        throw new IOException("Cannot remove root node");
      }
      if (!n.children.isEmpty()) {
        throw new NotEmptyException(path);
      }
    } finally {
      multiLock.unlock();
    }
    
    // outside the lock to avoid deadlock with update lock
    parent.removeChild(n.name, version);
  }

  @Override
  public void setData(String path, byte[] data, int version) throws NoSuchElementException, BadVersionException, IOException {
    if (data != null && data.length > juteMaxbuffer) {
      throw new IOException("Len error " + data.length);
    }
    multiLock.lock();
    Node n = null;
    try {
      n = traverse(path, false, CreateMode.PERSISTENT);
      if (n == null) {
        throw new NoSuchElementException(path);
      }
    } finally {
      multiLock.unlock();
    }
    n.setData(data, version);
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws BadVersionException, NoSuchElementException, AlreadyExistsException, IOException, KeeperException, InterruptedException {
    multiLock.lock();
    List<OpResult> res = new ArrayList<>();
    try {
      for (Op op : ops) {
        Record r = op.toRequestRecord();
        try {
          if (op instanceof Op.Check) {
            CheckVersionRequest rr = (CheckVersionRequest)r;
            Node n = traverse(rr.getPath(), false, CreateMode.PERSISTENT);
            if (n == null) {
              throw new NoSuchElementException(rr.getPath());
            }
            if (rr.getVersion() != -1 && n.version != rr.getVersion()) {
              throw new Exception("version mismatch");
            }
            // everything ok
            res.add(new OpResult.CheckResult());
          } else if (op instanceof Op.Create) {
            CreateRequest rr = (CreateRequest)r;
            createData(rr.getPath(), rr.getData(), CreateMode.fromFlag(rr.getFlags()));
            res.add(new OpResult.CreateResult(rr.getPath()));
          } else if (op instanceof Op.Delete) {
            DeleteRequest rr = (DeleteRequest)r;
            removeData(rr.getPath(), rr.getVersion());
            res.add(new OpResult.DeleteResult());
          } else if (op instanceof Op.SetData) {
            SetDataRequest rr = (SetDataRequest)r;
            setData(rr.getPath(), rr.getData(), rr.getVersion());
            VersionedData vd = getData(rr.getPath());
            Stat s = new Stat();
            s.setVersion(vd.getVersion());
            res.add(new OpResult.SetDataResult(s));
          } else {
            throw new Exception("Unknown Op: " + op);
          }
        } catch (Exception e) {
          res.add(new OpResult.ErrorResult(KeeperException.Code.APIERROR.intValue()));
        }
      }
    } finally {
      multiLock.unlock();
    }
    return res;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws InterruptedException, IOException {
    Map<String, Object> map = new HashMap<>();
    int version = 0;
    try {
      VersionedData data = getData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, watcher);
      if (data != null && data.getData() != null && data.getData().length > 0) {
        map = (Map<String, Object>) Utils.fromJSON(data.getData());
        version = data.getVersion();
      }
    } catch (NoSuchElementException e) {
      // ignore
    }
    map.put(AutoScalingParams.ZK_VERSION, version);
    return new AutoScalingConfig(map);
  }

  // ------------ simulator methods --------------

  public void simSetAutoScalingConfig(AutoScalingConfig cfg) throws Exception {
    try {
      makePath(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH);
    } catch (Exception e) {
      // ignore
    }
    setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(cfg), -1);
  }
}
