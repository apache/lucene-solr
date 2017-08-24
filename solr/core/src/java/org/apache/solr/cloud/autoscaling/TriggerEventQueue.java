package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Queue;

import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.autoscaling.ClusterDataProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TriggerEventQueue {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ENQUEUE_TIME = "_enqueue_time_";
  public static final String DEQUEUE_TIME = "_dequeue_time_";

  private final String triggerName;
  private final TimeSource timeSource;
  private final DistributedQueue delegate;

  public TriggerEventQueue(ClusterDataProvider clusterDataProvider, String triggerName, Overseer.Stats stats) throws IOException {
    // TODO: collect stats
    this.delegate = clusterDataProvider.getDistributedQueueFactory().makeQueue(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH + "/" + triggerName);
    this.triggerName = triggerName;
    this.timeSource = TimeSource.CURRENT_TIME;
  }

  public boolean offerEvent(TriggerEvent event) {
    event.getProperties().put(ENQUEUE_TIME, timeSource.getTime());
    try {
      byte[] data = Utils.toJSON(event);
      delegate.offer(data);
      return true;
    } catch (Exception e) {
      LOG.warn("Exception adding event " + event + " to queue " + triggerName, e);
      return false;
    }
  }

  public TriggerEvent peekEvent() {
    byte[] data;
    try {
      while ((data = delegate.peek()) != null) {
        if (data.length == 0) {
          LOG.warn("ignoring empty data...");
          continue;
        }
        try {
          Map<String, Object> map = (Map<String, Object>) Utils.fromJSON(data);
          return fromMap(map);
        } catch (Exception e) {
          LOG.warn("Invalid event data, ignoring: " + new String(data));
          continue;
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception peeking queue of trigger " + triggerName, e);
    }
    return null;
  }

  public TriggerEvent pollEvent() {
    byte[] data;
    try {
      while ((data = delegate.poll()) != null) {
        if (data.length == 0) {
          LOG.warn("ignoring empty data...");
          continue;
        }
        try {
          Map<String, Object> map = (Map<String, Object>) Utils.fromJSON(data);
          return fromMap(map);
        } catch (Exception e) {
          LOG.warn("Invalid event data, ignoring: " + new String(data));
          continue;
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception polling queue of trigger " + triggerName, e);
    }
    return null;
  }

  private TriggerEvent fromMap(Map<String, Object> map) {
    String id = (String)map.get("id");
    String source = (String)map.get("source");
    long eventTime = ((Number)map.get("eventTime")).longValue();
    TriggerEventType eventType = TriggerEventType.valueOf((String)map.get("eventType"));
    Map<String, Object> properties = (Map<String, Object>)map.get("properties");
    TriggerEvent res = new TriggerEvent(id, eventType, source, eventTime, properties);
    res.getProperties().put(DEQUEUE_TIME, timeSource.getTime());
    return res;
  }
}
