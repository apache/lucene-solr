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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;

/** Add this to a document to index lat/lon or x/y/z point, indexed as a dimensional value.
 *  Multiple values are allowed: just add multiple Geo3DPoint to the document with the
 *  same field name.
 *
 *  @lucene.experimental */
public final class Geo3DPoint extends Field {

  /** Indexing {@link FieldType}. */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(3, RamUsageEstimator.NUM_BYTES_INT);
    TYPE.freeze();
  }

  /** 
   * Creates a new Geo3DPoint field with the specified lat, lon (in radians), given a planet model.
   *
   * @throws IllegalArgumentException if the field name is null or lat or lon are out of bounds
   */
  public Geo3DPoint(String name, PlanetModel planetModel, double lat, double lon) {
    super(name, TYPE);
    // Translate lat/lon to x,y,z:
    final GeoPoint point = new GeoPoint(planetModel, lat, lon);
    fillFieldsData(planetModel.getMaximumMagnitude(), point.x, point.y, point.z);
  }

  /** 
   * Creates a new Geo3DPoint field with the specified x,y,z.
   *
   * @throws IllegalArgumentException if the field name is null or lat or lon are out of bounds
   */
  public Geo3DPoint(String name, PlanetModel planetModel, double x, double y, double z) {
    super(name, TYPE);
    fillFieldsData(planetModel.getMaximumMagnitude(), x, y, z);
  }

  private void fillFieldsData(double planetMax, double x, double y, double z) {
    byte[] bytes = new byte[12];
    NumericUtils.intToBytes(Geo3DUtil.encodeValue(planetMax, x), bytes, 0);
    NumericUtils.intToBytes(Geo3DUtil.encodeValue(planetMax, y), bytes, 1);
    NumericUtils.intToBytes(Geo3DUtil.encodeValue(planetMax, z), bytes, 2);
    fieldsData = new BytesRef(bytes);
  }
}
