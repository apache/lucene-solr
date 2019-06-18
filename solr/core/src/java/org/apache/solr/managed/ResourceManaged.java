package org.apache.solr.managed;

import java.util.Map;

/**
 *
 */
public interface ResourceManaged {

  String getName();

  void setManagedLimits(Limits limits);

  void setManagedLimit(String key, Limit limit);

  void setManagedLimitMax(String key, float max);

  void setManagedLimitMin(String key, float min);

  Limits getManagedLimits();

  Map<String, Float> getManagedValues();
}
