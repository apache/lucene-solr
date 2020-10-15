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

package org.apache.solr.client.solrj.request.json;

import java.util.HashMap;
import java.util.Map;

/**
 * A common parent for a small set of classes that allow easier composition of JSON facet objects.
 *
 * Designed for use with {@link JsonQueryRequest#withFacet(String, Map)}
 */
public abstract class JsonFacetMap<B extends JsonFacetMap<B>> extends HashMap<String, Object> {

  public abstract B getThis(); // Allows methods shared here to return subclass type

  public JsonFacetMap(String facetType) {
    super();

    put("type", facetType);
  }

  public B withDomain(DomainMap domain) {
    put("domain", domain);
    return getThis();
  }

  public B withSubFacet(String facetName,
                        @SuppressWarnings({"rawtypes"})JsonFacetMap map) {
    if (! containsKey("facet")) {
      put("facet", new HashMap<String, Object>());
    }

    @SuppressWarnings({"unchecked"})
    final Map<String, Object> subFacetMap = (Map<String, Object>) get("facet");
    subFacetMap.put(facetName, map);
    return getThis();
  }

  public B withStatSubFacet(String facetName, String statFacet) {
    if (! containsKey("facet")) {
      put("facet", new HashMap<String, Object>());
    }

    @SuppressWarnings({"unchecked"})
    final Map<String, Object> subFacetMap = (Map<String, Object>) get("facet");
    subFacetMap.put(facetName, statFacet);
    return getThis();
  }
}
