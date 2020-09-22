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
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class TriggerEventQueue {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ENQUEUE_TIME = "_enqueue_time_";
  public static final String DEQUEUE_TIME = "_dequeue_time_";

  private final String triggerName;
  private final TimeSource timeSource;
  private final DistributedQueue delegate;

  public TriggerEventQueue(SolrCloudManager cloudManager, String triggerName, Stats stats) throws IOException {
    // TODO: collect stats
    this.delegate = cloudManager.getDistributedQueueFactory().makeQueue(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH + "/" + triggerName);
    this.triggerName = triggerName;
    this.timeSource = cloudManager.getTimeSource();
  }

  public boolean offerEvent(TriggerEvent event) {
    event.getProperties().put(ENQUEUE_TIME, timeSource.getTimeNs());
    try {
      byte[] data = Utils.toJSON(event);
      delegate.offer(data);
      return true;
    } catch (Exception e) {
      log.warn("Exception adding event {} to queue {}", event, triggerName, e);
      return false;
    }
  }

  public TriggerEvent peekEvent() {
    byte[] data;
    try {
      while ((data = delegate.peek()) != null) {
        if (data.length == 0) {
          log.warn("ignoring empty data...");
          continue;
        }
        try {
          @SuppressWarnings({"unchecked"})
          Map<String, Object> map = (Map<String, Object>) Utils.fromJSON(data);
          return fromMap(map);
        } catch (Exception e) {
          log.warn("Invalid event data, ignoring: {}", new String(data, StandardCharsets.UTF_8));
          continue;
        }
      }
    } 
    catch (AlreadyClosedException e) {
      
    }
    catch (Exception e) {
      log.warn("Exception peeking queue of trigger {}", triggerName, e);
    }
    return null;
  }

  public TriggerEvent pollEvent() {
    byte[] data;
    try {
      while ((data = delegate.poll()) != null) {
        if (data.length == 0) {
          log.warn("ignoring empty data...");
          continue;
        }
        try {
          @SuppressWarnings({"unchecked"})
          Map<String, Object> map = (Map<String, Object>) Utils.fromJSON(data);
          return fromMap(map);
        } catch (Exception e) {
          log.warn("Invalid event data, ignoring: {}", new String(data, StandardCharsets.UTF_8));
          continue;
        }
      }
    } catch (Exception e) {
      log.warn("Exception polling queue of trigger {}", triggerName, e);
    }
    return null;
  }

  private TriggerEvent fromMap(Map<String, Object> map) {
    TriggerEvent res = TriggerEvent.fromMap(map);
    res.getProperties().put(DEQUEUE_TIME, timeSource.getTimeNs());
    return res;
  }
}
