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

import java.util.Map;

import org.apache.solr.core.CoreContainer;

/**
 * Provides additional context for the TriggerAction such as the trigger instance on
 * which the action is being executed as well as helper methods to pass computed information along
 * to the next action
 */
public class ActionContext {

  private final CoreContainer coreContainer;
  private final AutoScaling.Trigger source;
  private final Map<String, Object> properties;

  public ActionContext(CoreContainer coreContainer, AutoScaling.Trigger source, Map<String, Object> properties) {
    this.coreContainer = coreContainer;
    this.source = source;
    this.properties = properties;
  }

  public CoreContainer getCoreContainer() {
    return coreContainer;
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
}
