package org.apache.solr.managed;

import java.util.Map;

/**
 *
 */
public interface ResourceManagerPluginFactory {

  ResourceManagerPlugin create(String type, Map<String, Object> params) throws Exception;
}
