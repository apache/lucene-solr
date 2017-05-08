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
package org.apache.lucene.facet.sortedset;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.IndexOptions;

/** Add an instance of this to your Document for every facet
 *  label to be indexed via SortedSetDocValues. */
public class SortedSetDocValuesFacetField extends Field {
  
  /** Indexed {@link FieldType}. */
  public static final FieldType TYPE = new FieldType();
  static {
    // NOTE: we don't actually use these index options, because this field is "processed" by FacetsConfig.build()
    TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    TYPE.freeze();
  }

  /** Dimension. */
  public final String dim;

  /** Label. */
  public final String label;

  /** Sole constructor. */
  public SortedSetDocValuesFacetField(String dim, String label) {
    super("dummy", TYPE);
    FacetField.verifyLabel(label);
    FacetField.verifyLabel(dim);
    this.dim = dim;
    this.label = label;
  }

  @Override
  public String toString() {
    return "SortedSetDocValuesFacetField(dim=" + dim + " label=" + label + ")";
  }
}
