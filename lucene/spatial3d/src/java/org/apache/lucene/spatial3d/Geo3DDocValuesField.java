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
package org.apache.lucene.spatial3d;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;

import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.GeoPoint;

/** 
 * An per-document 3D location field.
 * <p>
 * Sorting by distance is efficient. Multiple values for the same field in one document
 * is allowed. 
 * <p>
 * This field defines static factory methods for common operations:
 * <ul>
 *   <li>TBD
 * </ul>
 * <p>
 * If you also need query operations, you should add a separate {@link Geo3DPoint} instance.
 * <p>
 * <b>WARNING</b>: Values are indexed with some loss of precision from the
 * original {@code double} values (4.190951585769653E-8 for the latitude component
 * and 8.381903171539307E-8 for longitude).
 * @see Geo3DPoint
 */
public class Geo3DDocValuesField extends Field {

  // These are the multiplicative constants we need to use to arrive at values that fit in 21 bits.
  // The formula we use to go from double to encoded value is:  Math.floor((value - minimum) * factor + 0.5)
  // If we plug in maximum for value, we should get 0x1FFFFF.
  // So, 0x1FFFFF = Math.floor((maximum - minimum) * factor + 0.5)
  // We factor out the 0.5 and Math.floor by stating instead:
  // 0x200000 = (maximum - minimum) * factor
  // So, factor = 0x200000 / (maximum - minimum)

  private final double inverseMaximumValue = 1.0 / (double)(0x200000);
  
  private final double inverseXFactor = (PlanetModel.WGS84.getMaximumXValue() - PlanetModel.WGS84.getMinimumXValue()) * inverseMaximumValue;
  private final double inverseYFactor = (PlanetModel.WGS84.getMaximumYValue() - PlanetModel.WGS84.getMinimumYValue()) * inverseMaximumValue;
  private final double inverseZFactor = (PlanetModel.WGS84.getMaximumZValue() - PlanetModel.WGS84.getMinimumZValue()) * inverseMaximumValue;
  
  private final double xFactor = 1.0 / inverseXFactor;
  private final double yFactor = 1.0 / inverseYFactor;
  private final double zFactor = 1.0 / inverseZFactor;
  
  /**
   * Type for a Geo3DDocValuesField
   * <p>
   * Each value stores a 64-bit long where the three values (x, y, and z) are given
   * 21 bits each.  Each 21-bit value represents the maximum extent in that dimension
   * for the WGS84 planet model.
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE.freeze();
  }
  
  /** 
   * Creates a new Geo3DDocValuesField with the specified x, y, and z
   * @param name field name
   * @param point is the point.
   * @throws IllegalArgumentException if the field name is null or the point is out of bounds
   */
  public Geo3DDocValuesField(final String name, final GeoPoint point) {
    super(name, TYPE);
    setLocationValue(point);
  }

  /** 
   * Creates a new Geo3DDocValuesField with the specified x, y, and z
   * @param name field name
   * @param x is the x value for the point.
   * @param y is the y value for the point.
   * @param z is the z value for the point.
   * @throws IllegalArgumentException if the field name is null or x, y, or z are out of bounds
   */
  public Geo3DDocValuesField(final String name, final double x, final double y, final double z) {
    super(name, TYPE);
    setLocationValue(x, y, z);
  }
  
  /**
   * Change the values of this field
   * @param point is the point.
   * @throws IllegalArgumentException if the point is out of bounds
   */
  public void setLocationValue(final GeoPoint point) {
    setLocationValue(point.x, point.y, point.z);
  }

  /**
   * Change the values of this field
   * @param x is the x value for the point.
   * @param y is the y value for the point.
   * @param z is the z value for the point.
   * @throws IllegalArgumentException if x, y, or z are out of bounds
   */
  public void setLocationValue(final double x, final double y, final double z) {
    int XEncoded = encodeX(x);
    int YEncoded = encodeY(y);
    int ZEncoded = encodeZ(z);
    fieldsData = Long.valueOf(
      (((long)(XEncoded & 0x1FFFFF)) << 42) |
      (((long)(YEncoded & 0x1FFFFF)) << 21) |
      ((long)(ZEncoded & 0x1FFFFF))
      );
  }

  // For encoding/decoding, we generally want the following behavior:
  // (1) If you encode the maximum value or the minimum value, the resulting int fits in 21 bits.
  // (2) If you decode an encoded value, you get back the original value for both the minimum and maximum planet model values.
  // (3) Rounding occurs such that a small delta from the minimum and maximum planet model values still returns the same
  // values -- that is, these are in the center of the range of input values that should return the minimum or maximum when decoded
  
  private int encodeX(final double x) {
    if (x > PlanetModel.WGS84.getMaximumXValue()) {
      throw new IllegalArgumentException("x value exceeds WGS84 maximum");
    } else if (x < PlanetModel.WGS84.getMinimumXValue()) {
      throw new IllegalArgumentException("x value less than WGS84 minimum");
    }
    return (int)Math.floor((x - PlanetModel.WGS84.getMinimumXValue()) * xFactor + 0.5);
  }
  
  private double decodeX(final int x) {
    return x * inverseXFactor + PlanetModel.WGS84.getMinimumXValue();
  }

  private int encodeY(final double y) {
    if (y > PlanetModel.WGS84.getMaximumYValue()) {
      throw new IllegalArgumentException("y value exceeds WGS84 maximum");
    } else if (y < PlanetModel.WGS84.getMinimumYValue()) {
      throw new IllegalArgumentException("y value less than WGS84 minimum");
    }
    return (int)Math.floor((y - PlanetModel.WGS84.getMinimumYValue()) * yFactor + 0.5);
  }

  private double decodeY(final int y) {
    return y * inverseYFactor + PlanetModel.WGS84.getMinimumYValue();
  }

  private int encodeZ(final double z) {
    if (z > PlanetModel.WGS84.getMaximumZValue()) {
      throw new IllegalArgumentException("z value exceeds WGS84 maximum");
    } else if (z < PlanetModel.WGS84.getMinimumZValue()) {
      throw new IllegalArgumentException("z value less than WGS84 minimum");
    }
    return (int)Math.floor((z - PlanetModel.WGS84.getMinimumZValue()) * zFactor + 0.5);
  }

  private double decodeZ(final int z) {
    return z * inverseZFactor + PlanetModel.WGS84.getMinimumZValue();
  }

  /** helper: checks a fieldinfo and throws exception if its definitely not a Geo3DDocValuesField */
  static void checkCompatible(FieldInfo fieldInfo) {
    // dv properties could be "unset", if you e.g. used only StoredField with this same name in the segment.
    if (fieldInfo.getDocValuesType() != DocValuesType.NONE && fieldInfo.getDocValuesType() != TYPE.docValuesType()) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with docValuesType=" + fieldInfo.getDocValuesType() + 
                                         " but this type has docValuesType=" + TYPE.docValuesType() + 
                                         ", is the field really a Geo3DDocValuesField?");
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
    
    result.append(decodeX(((int)(currentValue >> 42)) & 0x1FFFFF));
    result.append(',');
    result.append(decodeY(((int)(currentValue >> 21)) & 0x1FFFFF));
    result.append(',');
    result.append(decodeZ(((int)(currentValue)) & 0x1FFFFF));

    result.append('>');
    return result.toString();
  }

}
