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
package org.apache.solr.managed;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.core.SolrInfoBean;

/**
 * Convenience interface for {@link SolrInfoBean}-s that need to be managed.
 */
public interface ManagedMetricProducer extends SolrInfoBean, ManagedResource {

  @Override
  default Map<String, Float> getMonitoredValues(Collection<String> tags) {
    Map<String, Object> metrics = getMetricsSnapshot();
    if (metrics == null) {
      return Collections.emptyMap();
    }
    Map<String, Float> result = new HashMap<>();
    tags.forEach(tag -> {
      Object value = metrics.get(tag);
      if (value == null || !(value instanceof Number)) {
        return;
      }
      result.put(tag, ((Number)value).floatValue());
    });
    return result;
  }

  @Override
  default String getResourceName() {
    return getName();
  }
}
