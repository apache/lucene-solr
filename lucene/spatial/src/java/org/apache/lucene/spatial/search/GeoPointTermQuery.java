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
package org.apache.lucene.spatial.search;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.util.GeoUtils;
import org.apache.lucene.util.AttributeSource;

/**
 * TermQuery for GeoPointField for overriding {@link org.apache.lucene.search.MultiTermQuery} methods specific to
 * Geospatial operations
 *
 * @lucene.experimental
 */
abstract class GeoPointTermQuery extends MultiTermQuery {
  // simple bounding box optimization - no objects used to avoid dependencies
  /** minimum longitude value (in degrees) */
  protected final double minLon;
  /** minimum latitude value (in degrees) */
  protected final double minLat;
  /** maximum longitude value (in degrees) */
  protected final double maxLon;
  /** maximum latitude value (in degrees) */
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

  private static final RewriteMethod GEO_CONSTANT_SCORE_REWRITE = new RewriteMethod() {
    @Override
    public Query rewrite(IndexReader reader, MultiTermQuery query) {
      return new GeoPointTermQueryConstantScoreWrapper<>((GeoPointTermQuery)query);
    }
  };

  /** override package protected method */
  @Override
  protected abstract TermsEnum getTermsEnum(final Terms terms, AttributeSource atts) throws IOException;

  /** check if this instance equals another instance */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    GeoPointTermQuery that = (GeoPointTermQuery) o;

    if (Double.compare(that.minLon, minLon) != 0) return false;
    if (Double.compare(that.minLat, minLat) != 0) return false;
    if (Double.compare(that.maxLon, maxLon) != 0) return false;
    return Double.compare(that.maxLat, maxLat) == 0;

  }

  /** compute hashcode */
  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(minLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(minLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }
}
