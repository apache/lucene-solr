package org.apache.solr.managed.plugins;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.managed.AbstractResourceManagerPlugin;
import org.apache.solr.managed.ResourceManagerPool;
import org.apache.solr.search.SolrCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link org.apache.solr.managed.ResourceManagerPlugin} specific to
 * the management of {@link org.apache.solr.search.SolrCache} instances.
 * <p>This plugin calculates the total size and maxRamMB of all registered cache instances
 * and adjusts each cache's limits so that the aggregated values again fit within the pool limits.</p>
 * <p>In order to avoid thrashing the plugin uses a dead band (by default {@link #DEFAULT_DEAD_BAND}),
 * which can be adjusted using configuration parameter {@link #DEAD_BAND}. If monitored values don't
 * exceed the limits +/- the dead band then no action is taken.</p>
 */
public class CacheManagerPlugin extends AbstractResourceManagerPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String TYPE = "cache";

  public static final String DEAD_BAND = "deadBand";
  public static final float DEFAULT_DEAD_BAND = 0.1f;

  private static final Map<String, String> controlledToMonitored = new HashMap<>();

  static {
    controlledToMonitored.put(SolrCache.MAX_RAM_MB_PARAM, SolrCache.RAM_BYTES_USED_PARAM);
    controlledToMonitored.put(SolrCache.MAX_SIZE_PARAM, SolrCache.SIZE_PARAM);
  }

  private static final Collection<String> MONITORED_PARAMS = Arrays.asList(
      SolrCache.SIZE_PARAM,
      SolrCache.HIT_RATIO_PARAM,
      SolrCache.RAM_BYTES_USED_PARAM
  );

  private static final Collection<String> CONTROLLED_PARAMS = Arrays.asList(
      SolrCache.MAX_RAM_MB_PARAM,
      SolrCache.MAX_SIZE_PARAM
  );

  private float deadBand = DEFAULT_DEAD_BAND;

  @Override
  public Collection<String> getMonitoredParams() {
    return MONITORED_PARAMS;
  }

  @Override
  public Collection<String> getControlledParams() {
    return CONTROLLED_PARAMS;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public void init(Map<String, Object> params) {
    String deadBandStr = String.valueOf(params.getOrDefault(DEAD_BAND, DEFAULT_DEAD_BAND));
    try {
      deadBand = Float.parseFloat(deadBandStr);
    } catch (Exception e) {
      log.warn("Invalid deadBand parameter value '" + deadBandStr + "', using default " + DEFAULT_DEAD_BAND);
    }
  }

  @Override
  public void manage(ResourceManagerPool pool) throws Exception {
    Map<String, Map<String, Object>> currentValues = pool.getCurrentValues();
    Map<String, Number> totalValues = pool.getTotalValues();
    // pool limits are defined using controlled tags
    pool.getPoolLimits().forEach((poolLimitName, value) -> {
      // only numeric limits are supported
      if (value == null || !(value instanceof Number)) {
        return;
      }
      float poolLimitValue = ((Number)value).floatValue();
      if (poolLimitValue <= 0) {
        return;
      }
      String monitoredTag = controlledToMonitored.get(poolLimitName);
      if (monitoredTag == null) {
        return;
      }
      Number totalValue = totalValues.get(monitoredTag);
      if (totalValue == null || totalValue.floatValue() <= 0.0f) {
        return;
      }
      float totalDelta = poolLimitValue - totalValue.floatValue();

      // dead band to avoid thrashing
      if (Math.abs(totalDelta / poolLimitValue) < deadBand) {
        return;
      }

      float changeRatio = poolLimitValue / totalValue.floatValue();
      // modify current limits by the changeRatio
      pool.getResources().forEach((name, resource) -> {
        Map<String, Object> resourceLimits = resource.getResourceLimits();
        Object limit = resourceLimits.get(poolLimitName);
        // XXX we could attempt here to control eg. ramBytesUsed by adjusting maxSize limit
        // XXX and vice versa if the current limit is undefined or unsupported
        if (limit == null || !(limit instanceof Number)) {
          return;
        }
        float currentResourceLimit = ((Number)limit).floatValue();
        if (currentResourceLimit <= 0) {
          return;
        }
        float newLimit = currentResourceLimit * changeRatio;
        try {
          resource.setResourceLimit(poolLimitName, newLimit);
        } catch (Exception e) {
          log.warn("Failed to set managed limit " + poolLimitName +
              " from " + currentResourceLimit + " to " + newLimit + " on " + resource.getResourceId(), e);
        }
      });
    });
  }
}
