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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.NotEmptyException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.OverseerTriggerThread.MARKER_ACTIVE;
import static org.apache.solr.cloud.autoscaling.OverseerTriggerThread.MARKER_STATE;

/**
 * This plan simply removes nodeAdded and nodeLost markers from Zookeeper if their TTL has
 * expired. These markers are used by {@link NodeAddedTrigger} and {@link NodeLostTrigger} to
 * ensure fault tolerance in case of Overseer leader crash.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class InactiveMarkersPlanAction extends TriggerActionBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String TTL_PROP = "ttl";

  public static final int DEFAULT_TTL_SECONDS = 3600 * 24 * 2;

  private int cleanupTTL;

  public InactiveMarkersPlanAction() {
    super();
    TriggerUtils.validProperties(validProperties, TTL_PROP);
  }

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    super.configure(loader, cloudManager, properties);
    String cleanupStr = String.valueOf(properties.getOrDefault(TTL_PROP, String.valueOf(DEFAULT_TTL_SECONDS)));
    try {
      cleanupTTL = Integer.parseInt(cleanupStr);
    } catch (Exception e) {
      throw new TriggerValidationException(getName(), TTL_PROP, "invalid value '" + cleanupStr + "': " + e.toString());
    }
    if (cleanupTTL < 0) {
      throw new TriggerValidationException(getName(), TTL_PROP, "invalid value '" + cleanupStr + "', should be > 0. ");
    }
  }

  @Override
  public void process(TriggerEvent event, ActionContext context) throws Exception {
    if (log.isTraceEnabled()) {
      log.trace("-- {} cleaning markers", getName());
    }
    // use epoch time to track this across JVMs and nodes
    long currentTimeNs = cloudManager.getTimeSource().getEpochTimeNs();
    Map<String, Object> results = new LinkedHashMap<>();
    Set<String> cleanedUp = new TreeSet<>();
    cleanupMarkers(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH, currentTimeNs, cleanedUp);
    if (!cleanedUp.isEmpty()) {
      results.put("nodeAdded", cleanedUp);
      cleanedUp = new TreeSet<>();
    }
    cleanupMarkers(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH, currentTimeNs, cleanedUp);
    if (!cleanedUp.isEmpty()) {
      results.put("nodeLost", cleanedUp);
    }
    if (!results.isEmpty()) {
      context.getProperties().put(getName(), results);
    }
  }

  private void cleanupMarkers(String path, long currentTimeNs, Set<String> cleanedUp) throws Exception {
    DistribStateManager stateManager = cloudManager.getDistribStateManager();
    if (!stateManager.hasData(path)) {
      return;
    }
    List<String> markers = stateManager.listData(path);
    markers.forEach(m -> {
      String markerPath = path + "/" + m;
      try {
        Map<String, Object> payload = Utils.getJson(stateManager, markerPath);
        if (payload.isEmpty()) {
          log.trace(" -- ignore {}: either missing or unsupported format", markerPath);
          return;
        }
        boolean activeMarker = payload.getOrDefault(MARKER_STATE, MARKER_ACTIVE)
            .equals(MARKER_ACTIVE);
        long timestamp = ((Number)payload.get("timestamp")).longValue();
        long delta = TimeUnit.NANOSECONDS.toSeconds(currentTimeNs - timestamp);
        if (delta > cleanupTTL || !activeMarker) {
          try {
            stateManager.removeData(markerPath, -1);
            log.trace(" -- remove {}, delta={}, ttl={}, active={}", markerPath, delta, cleanupTTL, activeMarker);
            cleanedUp.add(m);
          } catch (NoSuchElementException nse) {
            // someone already removed it - ignore
            return;
          } catch (BadVersionException be) {
            throw new RuntimeException("should never happen", be);
          } catch (NotEmptyException ne) {
            log.error("Marker znode should be empty but it's not! Ignoring {} ({})", markerPath, ne);
          }
        } else {
          log.trace(" -- keep {}, delta={}, ttl={}, active={}", markerPath, delta, cleanupTTL, activeMarker);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (IOException | KeeperException e) {
        log.warn("Could not cleanup marker at {}, skipping... ", markerPath, e);
      }
    });
  }
}
