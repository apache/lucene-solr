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
package org.apache.solr.cloud.autoscaling;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@link org.apache.solr.cloud.autoscaling.AutoScaling.Trigger} implementations.
 * It handles state snapshot / restore in ZK.
 */
public abstract class TriggerBase implements AutoScaling.Trigger {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected SolrZkClient zkClient;
  protected Map<String,Object> lastState;


  protected TriggerBase(SolrZkClient zkClient) {
    this.zkClient = zkClient;
    try {
      zkClient.makePath(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH, false, true);
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Exception checking ZK path " + ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH, e);
    }
  }

  /**
   * Prepare and return internal state of this trigger in a format suitable for persisting in ZK.
   * @return map of internal state properties. Note: values must be supported by {@link Utils#toJSON(Object)}.
   */
  protected abstract Map<String,Object> getState();

  /**
   * Restore internal state of this trigger from properties retrieved from ZK.
   * @param state never null but may be empty.
   */
  protected abstract void setState(Map<String,Object> state);

  @Override
  public void saveState() {
    Map<String,Object> state = Utils.getDeepCopy(getState(), 10, false, true);
    if (lastState != null && lastState.equals(state)) {
      // skip saving if identical
      return;
    }
    byte[] data = Utils.toJSON(state);
    String path = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + getName();
    try {
      if (zkClient.exists(path, true)) {
        // update
        zkClient.setData(path, data, -1, true);
      } else {
        // create
        zkClient.create(path, data, CreateMode.PERSISTENT, true);
      }
      lastState = state;
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Exception updating trigger state '" + path + "'", e);
    }
  }

  @Override
  public void restoreState() {
    byte[] data = null;
    String path = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + getName();
    try {
      if (zkClient.exists(path, true)) {
        data = zkClient.getData(path, null, new Stat(), true);
      }
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Exception getting trigger state '" + path + "'", e);
    }
    if (data != null) {
      Map<String, Object> restoredState = (Map<String, Object>)Utils.fromJSON(data);
      // make sure lastState is sorted
      restoredState = Utils.getDeepCopy(restoredState, 10, false, true);
      setState(restoredState);
      lastState = restoredState;
    }
  }
}
