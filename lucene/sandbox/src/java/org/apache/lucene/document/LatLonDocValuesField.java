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
package org.apache.lucene.document;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;

/** 
 * An per-document location field.
 * <p>
 * Sorting by distance is efficient. Multiple values for the same field in one document
 * is allowed. 
 * <p>
 * This field defines static factory methods for common operations:
 * <ul>
 *   <li>{@link #newDistanceSort newDistanceSort()} for ordering documents by distance from a specified location. 
 * </ul>
 * <p>
 * If you also need query operations, you should add a separate {@link LatLonPoint} instance.
 * If you also need to store the value, you should add a separate {@link StoredField} instance.
 * <p>
 * <b>WARNING</b>: Values are indexed with some loss of precision from the
 * original {@code double} values (4.190951585769653E-8 for the latitude component
 * and 8.381903171539307E-8 for longitude).
 * @see LatLonPoint
 */
public class LatLonDocValuesField extends Field {

  /**
   * Type for a LatLonDocValuesField
   * <p>
   * Each value stores a 64-bit long where the upper 32 bits are the encoded latitude,
   * and the lower 32 bits are the encoded longitude.
   * @see org.apache.lucene.geo.GeoEncodingUtils#decodeLatitude(int)
   * @see org.apache.lucene.geo.GeoEncodingUtils#decodeLongitude(int)
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE.freeze();
  }
  
  /** 
   * Creates a new LatLonDocValuesField with the specified latitude and longitude
   * @param name field name
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of bounds
   */
  public LatLonDocValuesField(String name, double latitude, double longitude) {
    super(name, TYPE);
    setLocationValue(latitude, longitude);
  }
  
  /**
   * Change the values of this field
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @throws IllegalArgumentException if latitude or longitude are out of bounds
   */
  public void setLocationValue(double latitude, double longitude) {
    int latitudeEncoded = encodeLatitude(latitude);
    int longitudeEncoded = encodeLongitude(longitude);
    fieldsData = Long.valueOf((((long)latitudeEncoded) << 32) | (longitudeEncoded & 0xFFFFFFFFL));
  }

  /** helper: checks a fieldinfo and throws exception if its definitely not a LatLonDocValuesField */
  static void checkCompatible(FieldInfo fieldInfo) {
    // dv properties could be "unset", if you e.g. used only StoredField with this same name in the segment.
    if (fieldInfo.getDocValuesType() != DocValuesType.NONE && fieldInfo.getDocValuesType() != TYPE.docValuesType()) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with docValuesType=" + fieldInfo.getDocValuesType() + 
                                         " but this type has docValuesType=" + TYPE.docValuesType() + 
                                         ", is the field really a LatLonDocValuesField?");
    }
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    long currentValue = Long.valueOf((Long)fieldsData);
    result.append(decodeLatitude((int)(currentValue >> 32)));
    result.append(',');
    result.append(decodeLongitude((int)(currentValue & 0xFFFFFFFF)));

    result.append('>');
    return result.toString();
  }

  /**
   * Creates a SortField for sorting by distance from a location.
   * <p>
   * This sort orders documents by ascending distance from the location. The value returned in {@link FieldDoc} for
   * the hits contains a Double instance with the distance in meters.
   * <p>
   * If a document is missing the field, then by default it is treated as having {@link Double#POSITIVE_INFINITY} distance
   * (missing values sort last).
   * <p>
   * If a document contains multiple values for the field, the <i>closest</i> distance to the location is used.
   * 
   * @param field field name. must not be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @return SortField ordering documents by distance
   * @throws IllegalArgumentException if {@code field} is null or location has invalid coordinates.
   */
  public static SortField newDistanceSort(String field, double latitude, double longitude) {
    return new LatLonPointSortField(field, latitude, longitude);
  }
}
