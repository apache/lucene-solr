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
package org.apache.lucene.bkdtree;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;

/** Add this to a document to index lat/lon point, but be sure to use {@link BKDTreeDocValuesFormat} for the field.
 *
 * @deprecated Use dimensional values in Lucene 6.0 instead */
@Deprecated
public final class BKDPointField extends Field {

  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE.freeze();
  }

  /** 
   * Creates a new BKDPointField field with the specified lat and lon
   * @param name field name
   * @param lat double latitude
   * @param lon double longitude
   * @throws IllegalArgumentException if the field name is null or lat or lon are out of bounds
   */
  public BKDPointField(String name, double lat, double lon) {
    super(name, TYPE);
    if (BKDTreeWriter.validLat(lat) == false) {
      throw new IllegalArgumentException("invalid lat (" + lat + "): must be -90 to 90");
    }
    if (BKDTreeWriter.validLon(lon) == false) {
      throw new IllegalArgumentException("invalid lon (" + lon + "): must be -180 to 180");
    }
    fieldsData = Long.valueOf(((long) BKDTreeWriter.encodeLat(lat) << 32) | (BKDTreeWriter.encodeLon(lon) & 0xffffffffL));
  }
}
