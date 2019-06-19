package org.apache.solr.managed;

import java.util.Collection;
import java.util.Map;

/**
 *
 */
public interface ResourceManagerPlugin {

  String getType();

  void init(Map<String, Object> params);

  Collection<String> getMonitoredTags();
  Collection<String> getControlledTags();

  void manage(ResourceManagerPool pool);

}
