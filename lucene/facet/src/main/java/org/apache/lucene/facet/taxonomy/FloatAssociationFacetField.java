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
package org.apache.lucene.facet.taxonomy;

import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.util.BytesRef;

/** Add an instance of this to your {@link Document} to add
 *  a facet label associated with a float.  Use {@link
 *  TaxonomyFacetSumFloatAssociations} to aggregate float values
 *  per facet label at search time.
 * 
 *  @lucene.experimental */
public class FloatAssociationFacetField extends AssociationFacetField {

  /** Creates this from {@code dim} and {@code path} and a
   *  float association */
  public FloatAssociationFacetField(float assoc, String dim, String... path) {
    super(floatToBytesRef(assoc), dim, path);
  }

  /** Encodes a {@code float} as a 4-byte {@link BytesRef}. */
  public static BytesRef floatToBytesRef(float v) {
    return IntAssociationFacetField.intToBytesRef(Float.floatToIntBits(v));
  }

  /** Decodes a previously encoded {@code float}. */
  public static float bytesRefToFloat(BytesRef b) {
    return Float.intBitsToFloat(IntAssociationFacetField.bytesRefToInt(b));
  }

  @Override
  public String toString() {
    return "FloatAssociationFacetField(dim=" + dim + " path=" + Arrays.toString(path) + " value=" + bytesRefToFloat(assoc) + ")";
  }
}
