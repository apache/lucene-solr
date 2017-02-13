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
package org.apache.lucene.spatial.geopoint.search;

import java.util.Arrays;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.Polygon;

/** Implements a simple point in polygon query on a GeoPoint field. This is based on
 * {@code GeoPointInBBoxQueryImpl} and is implemented using a
 * three phase approach. First, like {@code GeoPointInBBoxQueryImpl}
 * candidate terms are queried using a numeric range based on the morton codes
 * of the min and max lat/lon pairs. Terms passing this initial filter are passed
 * to a secondary filter that verifies whether the decoded lat/lon point falls within
 * (or on the boundary) of the bounding box query. Finally, the remaining candidate
 * term is passed to the final point in polygon check.
 *
 * @see Polygon
 * @lucene.experimental
 * @deprecated Use the higher performance {@code LatLonPoint.newPolygonQuery} instead. */
@Deprecated
public final class GeoPointInPolygonQuery extends GeoPointInBBoxQuery {
  /** array of polygons being queried */
  final Polygon[] polygons;

  /** 
   * Constructs a new GeoPolygonQuery that will match encoded {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} terms
   * that fall within or on the boundary of the polygons defined by the input parameters. 
   */
  public GeoPointInPolygonQuery(String field, Polygon... polygons) {
    this(field, TermEncoding.PREFIX, polygons);
  }

  /**
   * Constructs a new GeoPolygonQuery that will match encoded {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} terms
   * that fall within or on the boundary of the polygon defined by the input parameters.
   * @deprecated Use {@link #GeoPointInPolygonQuery(String, Polygon[])}.
   */
  @Deprecated
  public GeoPointInPolygonQuery(final String field, final double[] polyLats, final double[] polyLons) {
    this(field, TermEncoding.PREFIX, polyLats, polyLons);
  }

  /**
   * Constructs a new GeoPolygonQuery that will match encoded {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} terms
   * that fall within or on the boundary of the polygon defined by the input parameters.
   * @deprecated Use {@link #GeoPointInPolygonQuery(String, GeoPointField.TermEncoding, Polygon[])} instead.
   */
  @Deprecated
  public GeoPointInPolygonQuery(final String field, final TermEncoding termEncoding, final double[] polyLats, final double[] polyLons) {
    this(field, termEncoding, new Polygon(polyLats, polyLons));
  }

  /** 
   * Constructs a new GeoPolygonQuery that will match encoded {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} terms
   * that fall within or on the boundary of the polygons defined by the input parameters. 
   */
  public GeoPointInPolygonQuery(String field, TermEncoding termEncoding, Polygon... polygons) {
    this(field, termEncoding, Rectangle.fromPolygon(polygons), polygons);
  }
  
  // internal constructor
  private GeoPointInPolygonQuery(String field, TermEncoding termEncoding, Rectangle boundingBox, Polygon... polygons) {
    super(field, termEncoding, boundingBox.minLat, boundingBox.maxLat, boundingBox.minLon, boundingBox.maxLon);
    this.polygons = polygons.clone();
  }

  /** throw exception if trying to change rewrite method */
  @Override
  public Query rewrite(IndexReader reader) {
    return new GeoPointInPolygonQueryImpl(field, termEncoding, this, this.minLat, this.maxLat, this.minLon, this.maxLon);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Arrays.hashCode(polygons);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    GeoPointInPolygonQuery other = (GeoPointInPolygonQuery) obj;
    if (!Arrays.equals(polygons, other.polygons)) return false;
    return true;
  }

  /** print out this polygon query */
  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (!getField().equals(field)) {
      sb.append(" field=");
      sb.append(getField());
      sb.append(':');
    }
    sb.append(" Polygon: ");
    sb.append(Arrays.toString(polygons));

    return sb.toString();
  }

  /**
   * API utility method for returning copy of the polygon array
   */
  public Polygon[] getPolygons() {
    return polygons.clone();
  }
}
