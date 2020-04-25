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

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;

/**
 * Sorts by distance from an origin location.
 */
final class XYPointSortField extends SortField {
  final float x;
  final float y;

  XYPointSortField(String field, float x, float y) {
    super(field, Type.CUSTOM);
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    this.x = x;
    this.y = y;
    setMissingValue(Double.POSITIVE_INFINITY);
  }
  
  @Override
  public FieldComparator<?> getComparator(int numHits, int sortPos) {
    return new XYPointDistanceComparator(getField(), x, y, numHits);
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
    temp = Float.floatToIntBits(x);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    temp = Float.floatToIntBits(y);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    XYPointSortField other = (XYPointSortField) obj;
    if (x != other.x || y != other.y) return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("<distance:");
    builder.append('"');
    builder.append(getField());
    builder.append('"');
    builder.append(" x=");
    builder.append(x);
    builder.append(" y=");
    builder.append(y);
    if (Double.POSITIVE_INFINITY != getMissingValue()) {
      builder.append(" missingValue=").append(getMissingValue());
    }
    builder.append('>');
    return builder.toString();
  }
}
