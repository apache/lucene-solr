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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.NamedList;

/**
 * Represents the top-level response for a bucket-based JSON facet (i.e. "terms" or "range")
 *
 * Allows access to JSON like:
 * <pre>
 *   {
 *     "numBuckets": 2,
 *     "buckets": [
 *       {...},
 *       {...}
 *     ]
 *   }
 * </pre>
 * <p>
 * Allows access to all top-level "terms" and "range" response properties (e.g. {@code allBuckets}, {@code numBuckets},
 * {@code before}, etc.)
 */
public class BucketBasedJsonFacet {
  public static final int UNSET_FLAG = -1;
  private List<BucketJsonFacet> buckets;
  private int numBuckets = UNSET_FLAG;
  private long allBuckets = UNSET_FLAG;
  private int beforeFirstBucketCount = UNSET_FLAG;
  private int afterLastBucketCount = UNSET_FLAG;
  private int betweenAllBucketsCount = UNSET_FLAG;

  public BucketBasedJsonFacet(NamedList<Object> bucketBasedFacet) {
    for (Map.Entry<String, Object> entry : bucketBasedFacet) {
      final String key = entry.getKey();
      final Object value = entry.getValue();
      if ("buckets".equals(key)) {
        final List<NamedList> bucketListUnparsed = (List<NamedList>) value;
        buckets = new ArrayList<>();
        for (NamedList bucket : bucketListUnparsed) {
          buckets.add(new BucketJsonFacet(bucket));
        }
      } else if ("numBuckets".equals(key)) {
        numBuckets = (int) value;
      } else if ("allBuckets".equals(key)) {
        allBuckets = (long) ((NamedList)value).get("count");
      } else if ("before".equals(key)) {
        beforeFirstBucketCount = (int) ((NamedList)value).get("count");
      } else if ("after".equals(key)) {
        afterLastBucketCount = (int) ((NamedList)value).get("count");
      } else if ("between".equals(key)) {
        betweenAllBucketsCount = (int) ((NamedList)value).get("count");
      } else {
        // We don't recognize the key.  Possible JSON faceting schema has changed without updating client.
        // Silently ignore for now, though we may want to consider throwing an error if this proves problematic.
      }
    }
  }

  /**
   * Retrieves the facet buckets returned by the server.
   */
  public List<BucketJsonFacet> getBuckets() {
    return buckets;
  }

  /**
   * The total number of buckets found in the domain (of which the returned buckets are only a part).
   *
   * This value can only be computed on "terms" facets where the user has specifically requested it with the
   * {@code numBuckets} option.  {@link #UNSET_FLAG} is returned if this is a "range" facet or {@code numBuckets}
   * computation was not requested in the intiial request.
   */
  public int getNumBuckets() {
    return numBuckets;
  }

  /**
   * The sum cardinality of all buckets in the "terms" facet.
   *
   * Note that for facets on multi-valued fields, documents may belong to multiple buckets, making {@link #getAllBuckets()}
   * return a result greater than the number of documents in the domain.
   * <p>
   * This value is only present if the user has specifically requested it with the {@code allBuckets} option.
   * {@link #UNSET_FLAG} is returned if this is not the case.
   */
  public long getAllBuckets() {
    return allBuckets;
  }

  /**
   * The count of all records whose field value precedes the {@code start} of this "range" facet
   *
   * This value is only present if the user has specifically requested it with the {@code other} option.
   * {@link #UNSET_FLAG} is returned if this is not the case.
   */
  public int getBefore() {
    return beforeFirstBucketCount;
  }

  /**
   * The count of all records whose field value follows the {@code end} of this "range" facet
   *
   * This value is only present if the user has specifically requested it with the {@code other} option.
   * {@link #UNSET_FLAG} is returned if this is not the case.
   */
  public int getAfter() {
    return afterLastBucketCount;
  }

  /**
   * The count of all records whose field value falls between {@code start} and {@code end}.
   *
   * This value is only present if the user has specifically requested it with the {@code other} option.
   * {@link #UNSET_FLAG} is returned if this is not the case.
   */
  public int getBetween() {
    return betweenAllBucketsCount;
  }
}
