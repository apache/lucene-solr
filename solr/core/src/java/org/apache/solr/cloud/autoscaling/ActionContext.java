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

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.MapWriter;

/**
 * Provides additional context for the TriggerAction such as the trigger instance on
 * which the action is being executed as well as helper methods to pass computed information along
 * to the next action.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class ActionContext implements MapWriter {

  private final SolrCloudManager cloudManager;
  private final AutoScaling.Trigger source;
  private final Map<String, Object> properties;

  public ActionContext(SolrCloudManager cloudManager, AutoScaling.Trigger source, Map<String, Object> properties) {
    this.cloudManager = cloudManager;
    this.source = source;
    this.properties = properties;
  }

  public SolrCloudManager getCloudManager() {
    return cloudManager;
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
