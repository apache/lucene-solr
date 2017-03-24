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

import java.util.Date;
import java.util.Map;

public class AutoScaling {

  public enum EventType {
    NODEADDED,
    NODELOST,
    REPLICALOST,
    MANUAL,
    SCHEDULED,
    SEARCHRATE,
    INDEXRATE
  }

  public enum TriggerStage {
    STARTED,
    ABORTED,
    SUCCEEDED,
    FAILED,
    BEFORE_ACTION,
    AFTER_ACTION
  }

  public static interface TriggerListener {
    public void triggerFired(Trigger trigger, Event event);
  }

  public static class HttpCallbackListener implements TriggerListener {
    @Override
    public void triggerFired(Trigger trigger, Event event) {

    }
  }

  public static interface Trigger {
    public String getName();

    public EventType getEventType();

    public boolean isEnabled();

    public Map<String, Object> getProperties();
  }

  public static interface Event {
    public String getSource();

    public Date getTime();

    public EventType getType();
  }

}
