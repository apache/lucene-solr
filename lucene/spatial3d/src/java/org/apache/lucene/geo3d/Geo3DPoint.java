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
package org.apache.lucene.geo3d;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Add this to a document to index lat/lon or x/y/z point, indexed as a 3D point.
 * Multiple values are allowed: just add multiple Geo3DPoint to the document with the
 * same field name.
 * <p>
 * This field defines static factory methods for creating a shape query:
 * <ul>
 *   <li>{@link #newShapeQuery newShapeQuery()} for matching all points inside a specified shape
 * </ul>
 *
 *  @lucene.experimental */
public final class Geo3DPoint extends Field {

  private final PlanetModel planetModel;

  /** Indexing {@link FieldType}. */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(3, Integer.BYTES);
    TYPE.freeze();
  }

  /** 
   * Creates a new Geo3DPoint field with the specified lat, lon (in radians), given a planet model.
   *
   * @throws IllegalArgumentException if the field name is null or lat or lon are out of bounds
   */
  public Geo3DPoint(String name, PlanetModel planetModel, double lat, double lon) {
    super(name, TYPE);
    this.planetModel = planetModel;
    // Translate lat/lon to x,y,z:
    final GeoPoint point = new GeoPoint(planetModel, lat, lon);
    fillFieldsData(planetModel, point.x, point.y, point.z);
  }

  /** 
   * Creates a new Geo3DPoint field with the specified x,y,z.
   *
   * @throws IllegalArgumentException if the field name is null or lat or lon are out of bounds
   */
  public Geo3DPoint(String name, PlanetModel planetModel, double x, double y, double z) {
    super(name, TYPE);
    this.planetModel = planetModel;
    fillFieldsData(planetModel, x, y, z);
  }

  private void fillFieldsData(PlanetModel planetModel, double x, double y, double z) {
    byte[] bytes = new byte[12];
    encodeDimension(planetModel, x, bytes, 0);
    encodeDimension(planetModel, y, bytes, Integer.BYTES);
    encodeDimension(planetModel, z, bytes, 2*Integer.BYTES);
    fieldsData = new BytesRef(bytes);
  }

  // public helper methods (e.g. for queries)
  
  /** Encode single dimension */
  public static void encodeDimension(PlanetModel planetModel, double value, byte bytes[], int offset) {
    NumericUtils.intToBytes(Geo3DUtil.encodeValue(planetModel.getMaximumMagnitude(), value), bytes, offset);
  }
  
  /** Decode single dimension */
  public static double decodeDimension(PlanetModel planetModel, byte value[], int offset) {
    return Geo3DUtil.decodeValueCenter(planetModel.getMaximumMagnitude(), NumericUtils.bytesToInt(value, offset));
  }

  /** Returns a query matching all points inside the provided shape.
   * 
   * @param planetModel The {@link PlanetModel} to use, which must match what was used during indexing
   * @param field field name. must not be {@code null}.
   * @param shape Which {@link GeoShape} to match
   */
  public static Query newShapeQuery(PlanetModel planetModel, String field, GeoShape shape) {
    return new PointInGeo3DShapeQuery(planetModel, field, shape);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    BytesRef bytes = (BytesRef) fieldsData;
    result.append(" x=" + decodeDimension(planetModel, bytes.bytes, bytes.offset));
    result.append(" y=" + decodeDimension(planetModel, bytes.bytes, bytes.offset + Integer.BYTES));
    result.append(" z=" + decodeDimension(planetModel, bytes.bytes, bytes.offset + 2*Integer.BYTES));
    result.append('>');
    return result.toString();
  }
}
