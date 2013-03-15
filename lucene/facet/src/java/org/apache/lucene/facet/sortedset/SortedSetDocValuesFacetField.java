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

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.util.BytesRef;

/** Add instances of this to your Document if you intend to
 *  use {@link SortedSetDocValuesAccumulator} to count facets
 *  at search time.  Note that this only supports flat
 *  facets (dimension + label).  Add multiple instances of
 *  this to your document, one per dimension + label, and
 *  it's fine if a given dimension is multi-valued. */

public class SortedSetDocValuesFacetField extends SortedSetDocValuesField {

  /** Create a {@code SortedSetDocValuesFacetField} with the
   *  provided {@link CategoryPath}. */
  public SortedSetDocValuesFacetField(CategoryPath cp)  {
    this(FacetIndexingParams.DEFAULT, cp);
  }

  /** Create a {@code SortedSetDocValuesFacetField} with the
   *  provided {@link CategoryPath}, and custom {@link
   *  FacetIndexingParams}. */
  public SortedSetDocValuesFacetField(FacetIndexingParams fip, CategoryPath cp)  {
    super(fip.getCategoryListParams(cp).field + SortedSetDocValuesReaderState.FACET_FIELD_EXTENSION, toBytesRef(fip, cp));
  }

  private static BytesRef toBytesRef(FacetIndexingParams fip, CategoryPath cp) {
    if (fip.getPartitionSize() != Integer.MAX_VALUE) {
      throw new IllegalArgumentException("partitions are not supported");
    }
    if (cp.length != 2) {
      throw new IllegalArgumentException("only flat facets (dimension + label) are currently supported");
    }
    String dimension = cp.components[0];
    char delim = fip.getFacetDelimChar();
    if (dimension.indexOf(delim) != -1) {
      throw new IllegalArgumentException("facet dimension cannot contain FacetIndexingParams.getFacetDelimChar()=" + delim + " (U+" + Integer.toHexString(delim) + "); got dimension=\"" + dimension + "\"");
    }

    // We can't use cp.toString(delim) because that fails if
    // cp.components[1] has the delim char, when in fact
    // that is allowed here (but not when using taxonomy
    // index):
    return new BytesRef(dimension + delim + cp.components[1]);
  }
}

