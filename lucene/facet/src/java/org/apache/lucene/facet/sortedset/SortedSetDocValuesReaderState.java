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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

/** Wraps a {@link IndexReader} and resolves ords
 *  using existing {@link SortedSetDocValues} APIs without a
 *  separate taxonomy index.  This only supports flat facets
 *  (dimension + label), and it makes faceting a bit
 *  slower, adds some cost at reopen time, but avoids
 *  managing the separate taxonomy index.  It also requires
 *  less RAM than the taxonomy index, as it manages the flat
 *  (2-level) hierarchy more efficiently.  In addition, the
 *  tie-break during faceting is now meaningful (in label
 *  sorted order).
 *
 *  <p><b>NOTE</b>: creating an instance of this class is
 *  somewhat costly, as it computes per-segment ordinal maps,
 *  so you should create it once and re-use that one instance
 *  for a given {@link IndexReader}. */

public final class SortedSetDocValuesReaderState {

  private final String field;
  private final AtomicReader topReader;
  private final int valueCount;
  final char separator;
  final String separatorRegex;

  /** Extension added to {@link CategoryListParams#field}
   *  to determin which field to read/write facet ordinals from/to. */
  public static final String FACET_FIELD_EXTENSION = "_sorted_doc_values";

  /** Holds start/end range of ords, which maps to one
   *  dimension (someday we may generalize it to map to
   *  hierarchies within one dimension). */
  static final class OrdRange {
    /** Start of range, inclusive: */
    public final int start;
    /** End of range, inclusive: */
    public final int end;

    /** Start and end are inclusive. */
    public OrdRange(int start, int end) {
      this.start = start;
      this.end = end;
    }
  }

  private final Map<String,OrdRange> prefixToOrdRange = new HashMap<String,OrdRange>();

  /** Create an instance, scanning the {@link
   *  SortedSetDocValues} from the provided reader, with
   *  default {@link FacetIndexingParams}. */
  public SortedSetDocValuesReaderState(IndexReader reader) throws IOException {
    this(FacetIndexingParams.DEFAULT, reader);
  }

  /** Create an instance, scanning the {@link
   *  SortedSetDocValues} from the provided reader and
   *  {@link FacetIndexingParams}. */
  public SortedSetDocValuesReaderState(FacetIndexingParams fip, IndexReader reader) throws IOException {

    this.field = fip.getCategoryListParams(null).field + FACET_FIELD_EXTENSION;
    this.separator = fip.getFacetDelimChar();
    this.separatorRegex = Pattern.quote(Character.toString(separator));

    // We need this to create thread-safe MultiSortedSetDV
    // per collector:
    if (reader instanceof AtomicReader) {
      topReader = (AtomicReader) reader;
    } else {
      topReader = new SlowCompositeReaderWrapper((CompositeReader) reader);
    }
    SortedSetDocValues dv = topReader.getSortedSetDocValues(field);
    if (dv == null) {
      throw new IllegalArgumentException("field \"" + field + "\" was not indexed with SortedSetDocValues");
    }
    if (dv.getValueCount() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("can only handle valueCount < Integer.MAX_VALUE; got " + dv.getValueCount());
    }
    valueCount = (int) dv.getValueCount();

    // TODO: we can make this more efficient if eg we can be
    // "involved" when OrdinalMap is being created?  Ie see
    // each term/ord it's assigning as it goes...
    String lastDim = null;
    int startOrd = -1;
    BytesRef spare = new BytesRef();

    // TODO: this approach can work for full hierarchy?;
    // TaxoReader can't do this since ords are not in
    // "sorted order" ... but we should generalize this to
    // support arbitrary hierarchy:
    for(int ord=0;ord<valueCount;ord++) {
      dv.lookupOrd(ord, spare);
      String[] components = spare.utf8ToString().split(separatorRegex, 2);
      if (components.length != 2) {
        throw new IllegalArgumentException("this class can only handle 2 level hierarchy (dim/value); got: " + spare.utf8ToString());
      }
      if (!components[0].equals(lastDim)) {
        if (lastDim != null) {
          prefixToOrdRange.put(lastDim, new OrdRange(startOrd, ord-1));
        }
        startOrd = ord;
        lastDim = components[0];
      }
    }

    if (lastDim != null) {
      prefixToOrdRange.put(lastDim, new OrdRange(startOrd, valueCount-1));
    }
  }

  SortedSetDocValues getDocValues() throws IOException {
    return topReader.getSortedSetDocValues(field);
  }

  OrdRange getOrdRange(String dim) {
    return prefixToOrdRange.get(dim);
  }

  String getField() {
    return field;
  }

  int getSize() {
    return valueCount;
  }
}
