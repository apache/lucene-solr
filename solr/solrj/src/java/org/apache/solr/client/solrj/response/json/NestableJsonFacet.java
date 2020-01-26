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

package org.apache.solr.client.solrj.response.json;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.util.NamedList;

/**
 * Represents the response to a "query" JSON facet.
 *
 * Relies on several other types to represent the variety of JSON facet responses.  The parsing of these responses
 * relies partially on the JSON property names.  When naming your facets in your request, avoid choosing names that
 * match existing values in the JSON faceting response schema, such as "count", "val", "minX", etc.
 */
public class NestableJsonFacet {
  private long domainCount;
  private final Map<String, NestableJsonFacet> queryFacetsByName;
  private final Map<String, BucketBasedJsonFacet> bucketBasedFacetByName;
  private final Map<String, Object> statsByName;
  private final Map<String, HeatmapJsonFacet> heatmapFacetsByName;

  public NestableJsonFacet(NamedList<Object> facetNL) {
    queryFacetsByName = new HashMap<>();
    bucketBasedFacetByName = new HashMap<>();
    heatmapFacetsByName = new HashMap<>();
    statsByName = new HashMap<>();

    for (Map.Entry<String, Object> entry : facetNL) {
      final String key = entry.getKey();
      if (getKeysToSkip().contains(key)) {
        continue;
      } else if ("count".equals(key)) {
        domainCount = ((Number) entry.getValue()).longValue();
      } else  if (entry.getValue() instanceof Number || entry.getValue() instanceof String ||
          entry.getValue() instanceof Date) {
        // Stat/agg facet value
        statsByName.put(key, entry.getValue());
      } else if(entry.getValue() instanceof NamedList) { // Either heatmap/query/range/terms facet
        final NamedList<Object> facet = (NamedList<Object>) entry.getValue();
        final boolean isBucketBased = facet.get("buckets") != null;
        final boolean isHeatmap = HeatmapJsonFacet.isHeatmapFacet(facet);
        if (isBucketBased) {
          bucketBasedFacetByName.put(key, new BucketBasedJsonFacet(facet));
        } else if (isHeatmap) {
          heatmapFacetsByName.put(key, new HeatmapJsonFacet(facet));
        } else { // "query" facet
          queryFacetsByName.put(key, new NestableJsonFacet(facet));
        }
      }
    }
  }

  /**
   * The number of records matching the domain of this facet.
   */
  public long getCount() {
    return domainCount;
  }

  /**
   * Retrieve a nested "query" facet by its name
   */
  public NestableJsonFacet getQueryFacet(String name) {
    return queryFacetsByName.get(name);
  }

  /**
   * @return the names of any "query" facets that are direct descendants of the current facet
   */
  public Set<String> getQueryFacetNames() {
    return queryFacetsByName.keySet();
  }

  /**
   * Retrieve a nested "terms" or "range" facet by its name.
   */
  public BucketBasedJsonFacet getBucketBasedFacets(String name) {
    return bucketBasedFacetByName.get(name);
  }

  /**
   * @return the names of any "terms" or "range" facets that are direct descendants of this facet
   */
  public Set<String> getBucketBasedFacetNames() {
    return bucketBasedFacetByName.keySet();
  }

  /**
   * Retrieve the value for a stat or agg with the provided name
   */
  public Object getStatValue(String name) {
    return statsByName.get(name);
  }

  /**
   * @return the names of any stat or agg that are direct descendants of this facet
   */
  public Set<String> getStatNames() {
    return statsByName.keySet();
  }

  /**
   * Retrieve a "heatmap" facet by its name
   */
  public HeatmapJsonFacet getHeatmapFacetByName(String name) {
    return heatmapFacetsByName.get(name);
  }

  /**
   * @return the names of any heatmap facets that are direct descendants of this facet
   */
  public Set<String> getHeatmapFacetNames() {
    return heatmapFacetsByName.keySet();
  }

  /*
   * Used by subclasses to control which keys are ignored during parsing.
   */
  protected Set<String> getKeysToSkip() {
    final HashSet<String> keysToSkip = new HashSet<>();
    return keysToSkip;
  }
}
