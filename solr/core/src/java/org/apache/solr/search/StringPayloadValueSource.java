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

package org.apache.solr.search;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.StrDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SimpleFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

public class StringPayloadValueSource extends ValueSource {
  protected final String field;
  protected final String val;
  protected final String indexedField;
  protected final BytesRef indexedBytes;
  protected final ValueSource defaultValueSource;

  public StringPayloadValueSource(String field, String val, String indexedField, BytesRef indexedBytes, ValueSource defaultValueSource) {
    this.field = field;
    this.val = val;
    this.indexedField = indexedField;
    this.indexedBytes = indexedBytes;
    this.defaultValueSource = defaultValueSource;
  }

  public SortField getSortField(boolean reverse) {
    return new StringPayloadValueSourceSortField(reverse);
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {

    final Terms terms = readerContext.reader().terms(indexedField);

    FunctionValues defaultValues = defaultValueSource.getValues(context, readerContext);

    // copied the bulk of this from TFValueSource - TODO: this is a very repeated pattern - base-class this advance logic stuff?
    return new StrDocValues(this) {
      PostingsEnum docs;
      int atDoc;
      int lastDocRequested = -1;
      String docValue = null;

      {
        reset();
      }

      public void reset() throws IOException {
        // no one should call us for deleted docs?

        if (terms != null) {
          final TermsEnum termsEnum = terms.iterator();
          if (termsEnum.seekExact(indexedBytes)) {
            docs = termsEnum.postings(null, PostingsEnum.ALL);
          } else {
            docs = null;
          }
        } else {
          docs = null;
        }

        if (docs == null) {
          // dummy PostingsEnum so floatVal() can work
          // when would this be called?  if field/val did not match?  this is called for every doc?  create once and cache?
          docs = new PostingsEnum() {
            @Override
            public int freq() {
              return 0;
            }

            @Override
            public int nextPosition() throws IOException {
              return -1;
            }

            @Override
            public int startOffset() throws IOException {
              return -1;
            }

            @Override
            public int endOffset() throws IOException {
              return -1;
            }

            @Override
            public BytesRef getPayload() throws IOException {
              return null;
            }

            @Override
            public int docID() {
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
              return 0;
            }
          };
        }
        atDoc = -1;
      }

      @Override
      public String strVal(int doc) {
        try {
          if (doc < lastDocRequested) {
            // out-of-order access.... reset
            reset();
          } else if (doc == lastDocRequested) {
            return docValue;
          }

          lastDocRequested = doc;

          if (atDoc < doc) {
            atDoc = docs.advance(doc);
          }

          if (atDoc > doc) {
            // term doesn't match this document... either because we hit the
            // end, or because the next doc is after this doc.
            docValue = defaultValues.strVal(doc);
            return docValue;
          }

          // a match!
          int freq = docs.freq();
          for (int i = 0; i < freq; i++) {
            docs.nextPosition();
            BytesRef payload = docs.getPayload();
            if (payload != null) {
              return bytesRefToString(payload);
            }
          }
          docValue = defaultValues.strVal(doc);
          return docValue;
        } catch (IOException e) {
          throw new RuntimeException("caught exception in function " + description() + " : doc=" + doc, e);
        }
      }
    };
  }

  private String bytesRefToString(BytesRef payload) {
    byte[] bytes = new byte[payload.length];
    System.arraycopy(payload.bytes, payload.offset, bytes, 0, payload.length);
    String ret = new String(bytes);
    return ret;
  }

  // TODO: should this be formalized at the ValueSource level?  Seems to be the convention
  public String name() {
    return "spayload";
  }

  @Override
  public String description() {
    return name() + '(' + field + ',' + val + ',' + defaultValueSource.toString() + ')';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    StringPayloadValueSource that = (StringPayloadValueSource) o;

    if (!indexedField.equals(that.indexedField)) return false;
    if (indexedBytes != null ? !indexedBytes.equals(that.indexedBytes) : that.indexedBytes != null) return false;
    return defaultValueSource.equals(that.defaultValueSource);

  }

  @Override
  public int hashCode() {
    int result = indexedField.hashCode();
    result = 31 * result + (indexedBytes != null ? indexedBytes.hashCode() : 0);
    result = 31 * result + defaultValueSource.hashCode();
    return result;
  }

  class StringPayloadValueSourceSortField extends SortField {
    public StringPayloadValueSourceSortField(boolean reverse) {
      super(description(), SortField.Type.REWRITEABLE, reverse);
    }

    @Override
    public SortField rewrite(IndexSearcher searcher) throws IOException {
      Map context = newContext(searcher);
      createWeight(context, searcher);
      return new SortField(getField(), new ValueSourceComparatorSource(context), getReverse());
    }
  }

  class ValueSourceComparatorSource extends FieldComparatorSource {
    private final Map context;

    public ValueSourceComparatorSource(Map context) {
      this.context = context;
    }

    @Override
    public FieldComparator<String> newComparator(String fieldname, int numHits,
                                                 int sortPos, boolean reversed) {
      return new ValueSourceComparator(context, numHits);
    }
  }

  /**
   * Implement a {@link org.apache.lucene.search.FieldComparator} that works
   * off of the {@link FunctionValues} for a ValueSource
   * instead of the normal Lucene FieldComparator that works off of a FieldCache.
   */
  class ValueSourceComparator extends SimpleFieldComparator<String> {
    private final String[] values;
    private final Map fcontext;
    private FunctionValues docVals;
    private String bottom;
    private String topValue;

    ValueSourceComparator(Map fcontext, int numHits) {
      this.fcontext = fcontext;
      values = new String[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      return values[slot1].compareTo(values[slot2]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return bottom.compareTo(docVals.strVal(doc));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      values[slot] = docVals.strVal(doc);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      docVals = getValues(fcontext, context);
    }

    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(final String value) {
      this.topValue = value;
    }

    @Override
    public String value(int slot) {
      return values[slot];
    }

    @Override
    public int compareTop(int doc) throws IOException {
      final String docValue = docVals.strVal(doc);
      return topValue.compareTo(docValue);
    }
  }
}
