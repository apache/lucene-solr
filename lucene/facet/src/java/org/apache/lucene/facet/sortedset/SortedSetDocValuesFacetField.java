package org.apache.lucene.facet.sortedset;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

/** Add an instance of this to your Document for every facet
 *  label to be indexed via SortedSetDocValues. */
public class SortedSetDocValuesFacetField implements IndexableField {
  
  public static final IndexableFieldType TYPE = new IndexableFieldType() {
    };

  @Override
  public String name() {
    return "dummy";
  }

  public IndexableFieldType fieldType() {
    return TYPE;
  }

  /** Dimension. */
  public final String dim;

  /** Label. */
  public final String label;

  /** Sole constructor. */
  public SortedSetDocValuesFacetField(String dim, String label) {
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
