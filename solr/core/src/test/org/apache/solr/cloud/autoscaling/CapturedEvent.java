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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;

/**
 *
 */
public class CapturedEvent {
  public final AutoScalingConfig.TriggerListenerConfig config;
  public final TriggerEventProcessorStage stage;
  public final String actionName;
  public final TriggerEvent event;
  public final String message;
  public final Map<String, Object> context = new HashMap<>();
  public final long timestamp;

  public CapturedEvent(long timestamp, ActionContext context, AutoScalingConfig.TriggerListenerConfig config, TriggerEventProcessorStage stage, String actionName,
                       TriggerEvent event, String message) {
    if (context != null) {
      context._forEachEntry((o, o2) -> CapturedEvent.this.context.put((String) o, o2));
      TriggerEvent.fixOps("properties." + TriggerEvent.REQUESTED_OPS, this.context);
      TriggerEvent.fixOps("properties." + TriggerEvent.UNSUPPORTED_OPS, this.context);
    }
    this.config = config;
    this.stage = stage;
    this.actionName = actionName;
    this.event = event;
    this.message = message;
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return "CapturedEvent{" +
        "timestamp=" + timestamp +
        ", stage=" + stage +
        ", actionName='" + actionName + '\'' +
        ", event=" + event +
        ", context=" + context +
        ", config=" + config +
        ", message='" + message + '\'' +
        '}';
  }
}
