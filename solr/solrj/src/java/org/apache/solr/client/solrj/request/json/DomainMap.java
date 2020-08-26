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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DomainMap extends HashMap<String, Object> {

  /**
   * Indicates that the domain should be narrowed by the specified filter
   *
   * May be called multiple times.  Each added filter is retained and used to narrow the domain.
   */
  public DomainMap withFilter(String filter) {
    if (filter == null) {
      throw new IllegalArgumentException("Parameter 'filter' must be non-null");
    }

    if (! containsKey("filter")) {
      put("filter", new ArrayList<String>());
    }

    @SuppressWarnings({"unchecked"})
    final List<String> filterList = (List<String>) get("filter");
    filterList.add(filter);
    return this;
  }

  /**
   * Indicates that the domain should be the following query
   *
   * May be called multiple times.  Each specified query is retained and included in the domain.
   */
  public DomainMap withQuery(String query) {
    if (query == null) {
      throw new IllegalArgumentException("Parameter 'query' must be non-null");
    }

    if (! containsKey("query")) {
      put("query", new ArrayList<String>());
    }

    @SuppressWarnings({"unchecked"})
    final List<String> queryList = (List<String>) get("query");
    queryList.add(query);
    return this;
  }

  /**
   * Provide a tag or tags that correspond to filters or queries to exclude from the domain
   *
   * May be called multiple times.  Each exclude-string is retained and used for removing queries/filters from the
   * domain specification.
   *
   * @param excludeTagsValue a comma-delimited String containing filter/query tags to exclude
   */
  public DomainMap withTagsToExclude(String excludeTagsValue) {
    if (excludeTagsValue == null) {
      throw new IllegalArgumentException("Parameter 'excludeTagValue' must be non-null");
    }

    if (! containsKey("excludeTags")) {
      put("excludeTags", new ArrayList<String>());
    }

    @SuppressWarnings({"unchecked"})
    final List<String> excludeTagsList = (List<String>) get("excludeTags");
    excludeTagsList.add(excludeTagsValue);
    return this;
  }

  /**
   * Indicates that the resulting domain will contain all parent documents of the children in the existing domain
   *
   * @param allParentsQuery a query used to identify all parent documents in the collection
   */
  public DomainMap setBlockParentQuery(String allParentsQuery) {
    if (allParentsQuery == null) {
      throw new IllegalArgumentException("Parameter 'allParentsQuery' must be non-null");
    }

    put("blockParent", allParentsQuery);
    return this;
  }

  /**
   * Indicates that the resulting domain will contain all child documents of the parents in the current domain
   *
   * @param allChildrenQuery a query used to identify all child documents in the collection
   */
  public DomainMap setBlockChildQuery(String allChildrenQuery) {
    if (allChildrenQuery == null) {
      throw new IllegalArgumentException("Parameter 'allChildrenQuery' must be non-null");
    }

    put("blockChildren", allChildrenQuery);
    return this;
  }

  /**
   * Transforms the domain by running a join query with the provided {@code from} and {@code to} parameters
   *
   * Join modifies the current domain by selecting the documents whose values in field {@code to} match values for the
   * field {@code from} in the current domain.
   *
   * @param from a field-name whose values are matched against {@code to} by the join
   * @param to a field name whose values should match values specified by the {@code from} field
   */
  public DomainMap setJoinTransformation(String from, String to) {
    if (from == null) {
      throw new IllegalArgumentException("Parameter 'from' must be non-null");
    }
    if (to == null) {
      throw new IllegalArgumentException("Parameter 'to' must be non-null");
    }

    final Map<String, Object> joinParameters = new HashMap<>();
    joinParameters.put("from", from);
    joinParameters.put("to", to);
    put("join", joinParameters);

    return this;
  }
}
