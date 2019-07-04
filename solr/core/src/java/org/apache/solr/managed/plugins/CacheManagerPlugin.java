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
 *
 */
public class CacheManagerPlugin extends AbstractResourceManagerPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String TYPE = "cache";

  public static final String SIZE_TAG = "size";
  public static final String HIT_RATIO_TAG = "hitratio";
  public static final String RAM_BYTES_USED_TAG = "ramBytesUsed";
  public static final String MAX_RAM_MB_TAG = "maxRamMB";

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

  @Override
  public Collection<String> getMonitoredTags() {
    return MONITORED_TAGS;
  }

  private static final Collection<String> CONTROLLED_TAGS = Arrays.asList(
      MAX_RAM_MB_TAG,
      SIZE_TAG
  );

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

      // 10% hysteresis to avoid thrashing
      // TODO: make the threshold configurable
      if (Math.abs(totalDelta / poolLimitValue) < 0.1f) {
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
