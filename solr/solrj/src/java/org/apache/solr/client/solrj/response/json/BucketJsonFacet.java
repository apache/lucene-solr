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

import java.util.HashSet;
import java.util.Set;

import org.apache.solr.common.util.NamedList;

/**
 * Represents an individual bucket result of a "term" or "range" facet.
 *
 * Allows access to JSON like:
 * <pre>
 *   {
 *     "val": "termX",
 *     "count": 10,
 *     "subfacetName": ...
 *   }
 * </pre>
 * <p>
 * Buckets may contain nested facets of any type.
 */
public class BucketJsonFacet extends NestableJsonFacet {
  private Object val;

  public BucketJsonFacet(NamedList<Object> singleBucket) {
    super(singleBucket); // sets "count", and stats or nested facets

    val = singleBucket.get("val");
  }

  /**
   * Retrieves the value (sometimes called the "key") of this bucket.
   *
   * The type of this object depends on the type of field being faceted on.  Usually a Date, Double, Integer, or String
   */
  public Object getVal() {
    return val;
  }

  @Override
  protected Set<String> getKeysToSkip() {
    final HashSet<String> keysToSkip = new HashSet<>();
    keysToSkip.add("val");
    return keysToSkip;
  }
}
