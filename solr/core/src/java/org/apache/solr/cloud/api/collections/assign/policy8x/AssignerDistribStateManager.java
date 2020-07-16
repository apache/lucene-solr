package org.apache.solr.cloud.api.collections.assign.policy8x;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.NotEmptyException;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.cloud.api.collections.assign.AssignerClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;

/**
 *
 */
public class AssignerDistribStateManager implements DistribStateManager {

  private final AutoScalingConfig autoScalingConfig;

  public AssignerDistribStateManager(AssignerClusterState assignerClusterState) {
    String autoscalingJson = (String) assignerClusterState.getProperties().get(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH);
    if (autoscalingJson != null) {
      autoScalingConfig = new AutoScalingConfig(autoscalingJson.getBytes(StandardCharsets.UTF_8));
    } else {
      autoScalingConfig = new AutoScalingConfig(Collections.emptyMap());
    }
  }

  @Override
  public boolean hasData(String path) throws IOException, KeeperException, InterruptedException {
    return false;
  }

  @Override
  public List<String> listData(String path) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    return null;
  }

  @Override
  public List<String> listData(String path, Watcher watcher) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    return null;
  }

  @Override
  public VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    return null;
  }

  @Override
  public void makePath(String path) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {

  }

  @Override
  public void makePath(String path, byte[] data, CreateMode createMode, boolean failOnExists) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {

  }

  @Override
  public String createData(String path, byte[] data, CreateMode mode) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {
    return null;
  }

  @Override
  public void removeData(String path, int version) throws NoSuchElementException, IOException, NotEmptyException, KeeperException, InterruptedException, BadVersionException {

  }

  @Override
  public void setData(String path, byte[] data, int version) throws BadVersionException, NoSuchElementException, IOException, KeeperException, InterruptedException {

  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws BadVersionException, NoSuchElementException, AlreadyExistsException, IOException, KeeperException, InterruptedException {
    return null;
  }

  @Override
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws InterruptedException, IOException {
    return autoScalingConfig;
  }

  @Override
  public void close() throws IOException {

  }
}
