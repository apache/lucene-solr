package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.GeoUtils;

/**
 * TermQuery for GeoPointField for overriding {@link org.apache.lucene.search.MultiTermQuery} methods specific to
 * Geospatial operations
 *
 * @lucene.experimental
 */
abstract class GeoPointTermQuery extends MultiTermQuery {
  // simple bounding box optimization - no objects used to avoid dependencies
  protected final double minLon;
  protected final double minLat;
  protected final double maxLon;
  protected final double maxLat;

  /**
   * Constructs a query matching terms that cannot be represented with a single
   * Term.
   */
  public GeoPointTermQuery(String field, final double minLon, final double minLat, final double maxLon, final double maxLat) {
    super(field);

    if (GeoUtils.isValidLon(minLon) == false) {
      throw new IllegalArgumentException("invalid minLon " + minLon);
    }
    if (GeoUtils.isValidLon(maxLon) == false) {
      throw new IllegalArgumentException("invalid maxLon " + maxLon);
    }
    if (GeoUtils.isValidLat(minLat) == false) {
      throw new IllegalArgumentException("invalid minLat " + minLat);
    }
    if (GeoUtils.isValidLat(maxLat) == false) {
      throw new IllegalArgumentException("invalid maxLat " + maxLat);
    }
    this.minLon = minLon;
    this.minLat = minLat;
    this.maxLon = maxLon;
    this.maxLat = maxLat;

    this.rewriteMethod = GEO_CONSTANT_SCORE_REWRITE;
  }

  public static final RewriteMethod GEO_CONSTANT_SCORE_REWRITE = new RewriteMethod() {
    @Override
    public Query rewrite(IndexReader reader, MultiTermQuery query) {
      return new GeoPointTermQueryConstantScoreWrapper<>((GeoPointTermQuery)query);
    }
  };


}
