package org.apache.solr.managed.plugins;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.managed.AbstractResourceManagerPlugin;
import org.apache.solr.managed.ResourceManagerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link org.apache.solr.managed.ResourceManagerPlugin} specific to
 * the management of {@link org.apache.solr.search.SolrCache} instances.
 * <p>This plugin calculates the total size and maxRamMB of all registered cache instances
 * and adjusts each cache's limits so that the aggregated values again fit within the pool limits.</p>
 * <p>In order to avoid thrashing the plugin uses a dead zone (by default {@link #DEFAULT_DEAD_ZONE}),
 * which can be adjusted using configuration parameter {@link #DEAD_ZONE}. If monitored values don't
 * exceed the limits +/- the dead zone no action is taken.</p>
 */
public class CacheManagerPlugin extends AbstractResourceManagerPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String TYPE = "cache";

  public static final String SIZE_TAG = "size";
  public static final String HIT_RATIO_TAG = "hitratio";
  public static final String RAM_BYTES_USED_TAG = "ramBytesUsed";
  public static final String MAX_RAM_MB_TAG = "maxRamMB";

  public static final String DEAD_ZONE = "deadZone";
  public static final float DEFAULT_DEAD_ZONE = 0.1f;

  private static final Map<String, String> controlledToMonitored = new HashMap<>();

  static {
    controlledToMonitored.put(MAX_RAM_MB_TAG, RAM_BYTES_USED_TAG);
    controlledToMonitored.put(SIZE_TAG, SIZE_TAG);
  }

  private static final Collection<String> MONITORED_TAGS = Arrays.asList(
      SIZE_TAG,
      HIT_RATIO_TAG,
      RAM_BYTES_USED_TAG
  );

  private static final Collection<String> CONTROLLED_TAGS = Arrays.asList(
      MAX_RAM_MB_TAG,
      SIZE_TAG
  );

  private float deadZone = DEFAULT_DEAD_ZONE;

  @Override
  public Collection<String> getMonitoredTags() {
    return MONITORED_TAGS;
  }

  @Override
  public Collection<String> getControlledTags() {
    return CONTROLLED_TAGS;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public void init(Map<String, Object> params) {
    String deadZoneStr = String.valueOf(params.getOrDefault(DEAD_ZONE, DEFAULT_DEAD_ZONE));
    try {
      deadZone = Float.parseFloat(deadZoneStr);
    } catch (Exception e) {
      log.warn("Invalid deadZone parameter value '" + deadZoneStr + "', using default " + DEFAULT_DEAD_ZONE);
    }
  }

  @Override
  public void manage(ResourceManagerPool pool) throws Exception {
    Map<String, Map<String, Float>> currentValues = pool.getCurrentValues();
    Map<String, Float> totalValues = pool.getTotalValues();
    // pool limits are defined using controlled tags
    pool.getPoolLimits().forEach((poolLimitName, poolLimitValue) -> {
      if (poolLimitValue == null || poolLimitValue <= 0) {
        return;
      }
      String monitoredTag = controlledToMonitored.get(poolLimitName);
      if (monitoredTag == null) {
        return;
      }
      Float totalValue = totalValues.get(monitoredTag);
      if (totalValue == null || totalValue <= 0.0f) {
        return;
      }
      float totalDelta = poolLimitValue - totalValue;

      // dead zone to avoid thrashing
      if (Math.abs(totalDelta / poolLimitValue) < deadZone) {
        return;
      }

      float changeRatio = poolLimitValue / totalValue;
      // modify current limits by the changeRatio
      pool.getResources().forEach((name, resource) -> {
        Map<String, Float> resourceLimits = resource.getManagedLimits();
        Float currentResourceLimit = resourceLimits.get(poolLimitName);
        if (currentResourceLimit == null || currentResourceLimit <= 0) {
          return;
        }
        float newLimit = currentResourceLimit * changeRatio;
        try {
          resource.setManagedLimit(poolLimitName, newLimit);
        } catch (Exception e) {
          log.warn("Failed to set managed limit " + poolLimitName +
              " from " + currentResourceLimit + " to " + newLimit + " on " + resource.getResourceName(), e);
        }
      });
    });
  }
}
