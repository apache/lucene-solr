package org.apache.solr.managed;

import java.util.Map;

import org.apache.solr.common.util.TimeSource;

/**
 *
 */
public class ProportionalResourceManager extends AbstractResourceManager {
  public ProportionalResourceManager(TimeSource timeSource) {
    super(timeSource);
  }

  @Override
  protected void managePool(Pool pool) {
    Map<String, Map<String, Float>> currentValues = pool.getCurrentValues();
    Map<String, Float> totalValues = pool.getTotalValues();
    Map<String, Float> totalCosts = pool.getTotalCosts();
    pool.getLimits().forEach(entry -> {
      Limit poolLimit = entry.getValue();
      Float totalValue = totalValues.get(entry.getKey());
      if (totalValue == null) {
        return;
      }
      float delta = poolLimit.deltaOutsideLimit(totalValue);
      Float totalCost = totalCosts.get(entry.getKey());
      if (totalCost == null || totalCost == 0) {
        return;
      }
      // re-adjust the limits based on relative costs
      pool.getResources().forEach((name, resource) -> {
        Map<String, Float> current = currentValues.get(name);
        if (current == null) {
          return;
        }
        Limits limits = resource.getManagedLimits();
        Limit limit = limits.getLimit(entry.getKey());
        if (limit == null) {
          return;
        }
        float newMax = limit.max - delta * limit.cost / totalCost;
        resource.setManagedLimitMax(entry.getKey(), newMax);
      });
    });
  }

}
