package org.apache.lucene.bkdtree3d;

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

import org.apache.lucene.geo3d.PlanetModel;
import org.apache.lucene.geo3d.GeoPoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;

// TODO: allow multi-valued, packing all points into a single BytesRef

/** Add this to a document to index lat/lon point, but be sure to use {@link Geo3DDocValuesFormat} for the field.

 *  @lucene.experimental */
public final class Geo3DPointField extends Field {

  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValuesType(DocValuesType.BINARY);
    TYPE.freeze();
  }

  /** 
   * Creates a new Geo3DPointField field with the specified lat, lon (in radians), given a planet model.
   *
   * @throws IllegalArgumentException if the field name is null or lat or lon are out of bounds
   */
  public Geo3DPointField(String name, final PlanetModel planetModel, final double lat, final double lon) {
    super(name, TYPE);
    final GeoPoint point = new GeoPoint(planetModel, lat, lon);
    byte[] bytes = new byte[12];
    Geo3DDocValuesFormat.writeInt(Geo3DDocValuesFormat.encodeValue(point.x), bytes, 0);
    Geo3DDocValuesFormat.writeInt(Geo3DDocValuesFormat.encodeValue(point.y), bytes, 4);
    Geo3DDocValuesFormat.writeInt(Geo3DDocValuesFormat.encodeValue(point.z), bytes, 8);
    fieldsData = new BytesRef(bytes);
  }

  /** 
   * Creates a new Geo3DPointField field with the specified x,y,z.
   *
   * @throws IllegalArgumentException if the field name is null or lat or lon are out of bounds
   */
  public Geo3DPointField(String name, double x, double y, double z) {
    super(name, TYPE);
    byte[] bytes = new byte[12];
    Geo3DDocValuesFormat.writeInt(Geo3DDocValuesFormat.encodeValue(x), bytes, 0);
    Geo3DDocValuesFormat.writeInt(Geo3DDocValuesFormat.encodeValue(y), bytes, 4);
    Geo3DDocValuesFormat.writeInt(Geo3DDocValuesFormat.encodeValue(z), bytes, 8);
    fieldsData = new BytesRef(bytes);
  }
}
