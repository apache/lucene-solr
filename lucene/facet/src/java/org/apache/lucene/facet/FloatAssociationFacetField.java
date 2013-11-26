package org.apache.lucene.facet;

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

import java.util.Arrays;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.BytesRef;

/** Associates an arbitrary float with the added facet
 *  path, encoding the float into a 4-byte BytesRef. */
public class FloatAssociationFacetField extends AssociationFacetField {

  /** Utility ctor: associates an int value (translates it
   *  to 4-byte BytesRef). */
  public FloatAssociationFacetField(float assoc, String dim, String... path) {
    super(floatToBytesRef(assoc), dim, path);
  }

  public static BytesRef floatToBytesRef(float v) {
    return IntAssociationFacetField.intToBytesRef(Float.floatToIntBits(v));
  }

  public static float bytesRefToFloat(BytesRef b) {
    return Float.intBitsToFloat(IntAssociationFacetField.bytesRefToInt(b));
  }

  @Override
  public String toString() {
    return "FloatAssociationFacetField(dim=" + dim + " path=" + Arrays.toString(path) + " value=" + bytesRefToFloat(assoc) + ")";
  }
}
