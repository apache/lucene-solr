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

import org.apache.lucene.document.Document; // javadocs
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;

/** Add an instance of this to your {@link Document} to add
 *  a facet label associated with an arbitrary byte[].
 *  This will require a custom {@link Facets}
 *  implementation at search time; see {@link
 *  IntAssociationFacetField} and {@link
 *  FloatAssociationFacetField} to use existing {@link
 *  Facets} implementations.
 * 
 *  @lucene.experimental */
public class AssociationFacetField extends Field {
  
  /** Indexed {@link FieldType}. */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    TYPE.freeze();
  }
  
  /** Dimension for this field. */
  public final String dim;

  /** Facet path for this field. */
  public final String[] path;

  /** Associated value. */
  public final BytesRef assoc;

  /** Creates this from {@code dim} and {@code path} and an
   *  association */
  public AssociationFacetField(BytesRef assoc, String dim, String... path) {
    super("dummy", TYPE);
    FacetField.verifyLabel(dim);
    for(String label : path) {
      FacetField.verifyLabel(label);
    }
    this.dim = dim;
    this.assoc = assoc;
    if (path.length == 0) {
      throw new IllegalArgumentException("path must have at least one element");
    }
    this.path = path;
  }

  @Override
  public String toString() {
    return "AssociationFacetField(dim=" + dim + " path=" + Arrays.toString(path) + " bytes=" + assoc + ")";
  }
}
