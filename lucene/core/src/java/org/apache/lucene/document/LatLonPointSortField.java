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

import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;

/**
 * Sorts by distance from an origin location.
 */
final class LatLonPointSortField extends SortField {
  final double latitude;
  final double longitude;

  LatLonPointSortField(String field, double latitude, double longitude) {
    super(field, SortField.Type.CUSTOM);
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
    this.latitude = latitude;
    this.longitude = longitude;
    setMissingValue(Double.POSITIVE_INFINITY);
  }
  
  @Override
  public FieldComparator<?> getComparator(int numHits, int sortPos) {
    return new LatLonPointDistanceComparator(getField(), latitude, longitude, numHits);
  }

  @Override
  public Double getMissingValue() {
    return (Double) super.getMissingValue();
  }

  @Override
  public void setMissingValue(Object missingValue) {
    if (Double.valueOf(Double.POSITIVE_INFINITY).equals(missingValue) == false) {
      throw new IllegalArgumentException("Missing value can only be Double.POSITIVE_INFINITY (missing values last), but got " + missingValue);
    }
    this.missingValue = missingValue;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(latitude);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(longitude);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    LatLonPointSortField other = (LatLonPointSortField) obj;
    if (Double.doubleToLongBits(latitude) != Double.doubleToLongBits(other.latitude)) return false;
    if (Double.doubleToLongBits(longitude) != Double.doubleToLongBits(other.longitude)) return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("<distance:");
    builder.append('"');
    builder.append(getField());
    builder.append('"');
    builder.append(" latitude=");
    builder.append(latitude);
    builder.append(" longitude=");
    builder.append(longitude);
    if (Double.POSITIVE_INFINITY != getMissingValue()) {
      builder.append(" missingValue=").append(getMissingValue());
    }
    builder.append('>');
    return builder.toString();
  }
}
