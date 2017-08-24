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
import java.util.Map;

import org.apache.solr.client.solrj.cloud.autoscaling.ClusterDataProvider;
import org.apache.solr.common.MapWriter;
import org.apache.solr.core.CoreContainer;

/**
 * Provides additional context for the TriggerAction such as the trigger instance on
 * which the action is being executed as well as helper methods to pass computed information along
 * to the next action
 */
public class ActionContext implements MapWriter {

  private final ClusterDataProvider clusterDataProvider;
  private final AutoScaling.Trigger source;
  private final Map<String, Object> properties;

  public ActionContext(ClusterDataProvider clusterDataProvider, AutoScaling.Trigger source, Map<String, Object> properties) {
    this.clusterDataProvider = clusterDataProvider;
    this.source = source;
    this.properties = properties;
  }

  public ClusterDataProvider getClusterDataProvider() {
    return clusterDataProvider;
  }

  public AutoScaling.Trigger getSource() {
    return source;
  }

  public Map<String, Object> getProperties()  {
    return properties;
  }

  public Object getProperty(String name)  {
    return properties != null ? properties.get(name) : null;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("source", source.getName());
    if (properties != null) {
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        ew.put("properties." + entry.getKey(), entry.getValue());
      }
    }
  }
}
