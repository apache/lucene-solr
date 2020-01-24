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

import java.util.Date;
import java.util.Map;

/**
 * Represents a "range" facet in a JSON request query.
 *
 * Ready for use with {@link JsonQueryRequest#withFacet(String, Map)}
 */
public class RangeFacetMap extends JsonFacetMap<RangeFacetMap> {
  public RangeFacetMap(String field, long start, long end, long gap) {
    super("range");

    if (field == null) {
      throw new IllegalArgumentException("Parameter 'field' must be non-null");
    }
    if (end < start) {
      throw new IllegalArgumentException("Parameter 'end' must be greater than parameter 'start'");
    }
    if (gap <= 0) {
      throw new IllegalArgumentException("Parameter 'gap' must be a positive integer");
    }

    put("field", field);
    put("start", start);
    put("end", end);
    put("gap", gap);
  }

  public RangeFacetMap(String field, double start, double end, double gap) {
    super("range");

    if (field == null) {
      throw new IllegalArgumentException("Parameter 'field' must be non-null");
    }
    if (end < start) {
      throw new IllegalArgumentException("Parameter 'end' must be greater than parameter 'start'");
    }
    if (gap <= 0) {
      throw new IllegalArgumentException("Parameter 'gap' must be a positive value");
    }

    put("field", field);
    put("start", start);
    put("end", end);
    put("gap", gap);
  }

  /**
   * Creates a "range" facet representation for a date field
   *
   * @param gap a DateMathParser compatible time/interval String.  (e.g. "+1MONTH")
   */
  public RangeFacetMap(String field, Date start, Date end, String gap) {
    super("range");

    if (field == null) {
      throw new IllegalArgumentException("Parameter 'field' must be non-null");
    }
    if (start == null) {
      throw new IllegalArgumentException("Parameter 'start' must be non-null");
    }
    if (end == null) {
      throw new IllegalArgumentException("Parameter 'gap' must be non-null");
    }

    put("field", field);
    put("start", start);
    put("end", end);
    put("gap", gap);
  }

  @Override
  public RangeFacetMap getThis() { return this; }

  /**
   * Indicates whether the facet's last bucket should stop exactly at {@code end}, or be extended to be {@code gap} wide
   *
   * Defaults to false if not specified.
   *
   * @param hardEnd true if the final bucket should be truncated at {@code end}; false otherwise
   */
  public RangeFacetMap setHardEnd(boolean hardEnd) {
    put("hardend", hardEnd);
    return this;
  }

  public enum OtherBuckets {
    BEFORE("before"), AFTER("after"), BETWEEN("between"), NONE("none"), ALL("all");

    private final String value;

    OtherBuckets(String value) {
      this.value = value;
    }

    public String toString() { return value; }
  }

  /**
   * Indicates that an additional range bucket(s) should be computed and added to those computed for {@code start} and {@code end}
   *
   * See {@link OtherBuckets} for possible options.
   */
  public RangeFacetMap setOtherBuckets(OtherBuckets bucketSpecifier) {
    if (bucketSpecifier == null) {
      throw new IllegalArgumentException("Parameter 'bucketSpecifier' must be non-null");
    }
    put("other", bucketSpecifier.toString());
    return this;
  }

  /**
   * Indicates that buckets should be returned only if they have a count of at least {@code minOccurrences}
   *
   * Defaults to '0' if not specified.
   */
  public RangeFacetMap setMinCount(int minOccurrences) {
    if (minOccurrences < 0) {
      throw new IllegalArgumentException(" Parameter 'minOccurrences' must be non-negative");
    }
    put("mincount", minOccurrences);
    return this;
  }
}
