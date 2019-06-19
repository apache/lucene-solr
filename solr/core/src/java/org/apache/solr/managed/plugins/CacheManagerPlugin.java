package org.apache.solr.managed.plugins;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.managed.AbstractResourceManagerPlugin;
import org.apache.solr.managed.ResourceManagerPool;

/**
 *
 */
public class CacheManagerPlugin extends AbstractResourceManagerPlugin {
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
  public void manage(ResourceManagerPool pool) {
    Map<String, Map<String, Float>> currentValues = pool.getCurrentValues();
    Map<String, Float> totalValues = pool.getTotalValues();
    pool.getLimits().forEach((poolLimitName, poolLimitValue) -> {
      String monitoredTag = controlledToMonitored.get(poolLimitName);
      if (monitoredTag == null) {
        return;
      }
      Float totalValue = totalValues.get(monitoredTag);
      if (totalValue == null) {
        return;
      }
      float totalDelta = poolLimitValue - totalValue;
      pool.getResources().forEach((name, resource) -> {
        Map<String, Float> current = currentValues.get(name);
        if (current == null) {
          return;
        }
        Map<String, Float> limits = resource.getManagedLimits();
        Float managedSize = limits.get(SIZE_TAG);
        Float resMaxRamMB = limits.get(MAX_RAM_MB_TAG);
        Float currentSize = current.get(SIZE_TAG);
        Float currentHitratio = current.get(HIT_RATIO_TAG);
        Float ramBytesUsed = current.get(RAM_BYTES_USED_TAG);

        // logic to adjust per-resource controlled limits
        if (poolLimitName.equals(MAX_RAM_MB_TAG)) {
          // adjust per-resource maxRamMB
        } else if (poolLimitName.equals(SIZE_TAG)) {
          // adjust per-resource size
        }
      });
    });
  }
}
