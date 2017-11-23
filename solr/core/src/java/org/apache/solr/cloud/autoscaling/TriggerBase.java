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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;

import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@link org.apache.solr.cloud.autoscaling.AutoScaling.Trigger} implementations.
 * It handles state snapshot / restore in ZK.
 */
public abstract class TriggerBase implements AutoScaling.Trigger {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final String name;
  protected final SolrCloudManager cloudManager;
  protected final DistribStateManager stateManager;
  protected final Map<String, Object> properties = new HashMap<>();
  protected final TriggerEventType eventType;
  protected final int waitForSecond;
  protected Map<String,Object> lastState;
  protected final AtomicReference<AutoScaling.TriggerEventProcessor> processorRef = new AtomicReference<>();
  protected final List<TriggerAction> actions;
  protected final boolean enabled;
  protected boolean isClosed;


  protected TriggerBase(TriggerEventType eventType, String name, Map<String, Object> properties, SolrResourceLoader loader, SolrCloudManager cloudManager) {
    this.eventType = eventType;
    this.name = name;
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
    if (properties != null) {
      this.properties.putAll(properties);
    }
    this.enabled = Boolean.parseBoolean(String.valueOf(this.properties.getOrDefault("enabled", "true")));
    this.waitForSecond = ((Number) this.properties.getOrDefault("waitFor", -1L)).intValue();
    List<Map<String, String>> o = (List<Map<String, String>>) properties.get("actions");
    if (o != null && !o.isEmpty()) {
      actions = new ArrayList<>(3);
      for (Map<String, String> map : o) {
        TriggerAction action = loader.newInstance(map.get("class"), TriggerAction.class);
        actions.add(action);
      }
    } else {
      actions = Collections.emptyList();
    }

    try {
      if (!stateManager.hasData(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH)) {
        stateManager.makePath(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
      }
    } catch (AlreadyExistsException e) {
      // ignore
    } catch (InterruptedException | KeeperException | IOException e) {
      LOG.warn("Exception checking ZK path " + ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH, e);
    }
  }

  @Override
  public void init() {
    List<Map<String, String>> o = (List<Map<String, String>>) properties.get("actions");
    if (o != null && !o.isEmpty()) {
      for (int i = 0; i < o.size(); i++) {
        Map<String, String> map = o.get(i);
        actions.get(i).init(map);
      }
    }
  }

  @Override
  public void setProcessor(AutoScaling.TriggerEventProcessor processor) {
    processorRef.set(processor);
  }

  @Override
  public AutoScaling.TriggerEventProcessor getProcessor() {
    return processorRef.get();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public TriggerEventType getEventType() {
    return eventType;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public int getWaitForSecond() {
    return waitForSecond;
  }

  @Override
  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public List<TriggerAction> getActions() {
    return actions;
  }

  @Override
  public boolean isClosed() {
    synchronized (this) {
      return isClosed;
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      isClosed = true;
      IOUtils.closeWhileHandlingException(actions);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, properties);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj.getClass().equals(this.getClass())) {
      TriggerBase that = (TriggerBase) obj;
      return this.name.equals(that.name)
          && this.properties.equals(that.properties);
    }
    return false;
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
      if (stateManager.hasData(path)) {
        // update
        stateManager.setData(path, data, -1);
      } else {
        // create
        stateManager.createData(path, data, CreateMode.PERSISTENT);
      }
      lastState = state;
    } catch (InterruptedException | BadVersionException | AlreadyExistsException | IOException | KeeperException e) {
      LOG.warn("Exception updating trigger state '" + path + "'", e);
    }
  }

  @Override
  public void restoreState() {
    byte[] data = null;
    String path = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + getName();
    try {
      if (stateManager.hasData(path)) {
        VersionedData versionedData = stateManager.getData(path);
        data = versionedData.getData();
      }
    } catch (Exception e) {
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
