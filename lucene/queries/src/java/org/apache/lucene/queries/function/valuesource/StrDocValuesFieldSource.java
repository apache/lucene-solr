package org.apache.lucene.queries.function.valuesource;

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
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.StrDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;

/**
 * A {@link ValueSource} for binary {@link DocValues} that represent an UTF-8
 * encoded String using:<ul>
 * <li>{@link org.apache.lucene.index.DocValues.Type#BYTES_FIXED_DEREF},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#BYTES_FIXED_STRAIGHT},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#BYTES_VAR_DEREF},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#BYTES_VAR_STRAIGHT},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#BYTES_FIXED_SORTED},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#BYTES_VAR_SORTED}.</li></ul>
 * <p>
 * If the segment has no {@link DocValues}, the default
 * {@link org.apache.lucene.index.DocValues.Source} of type
 * {@link org.apache.lucene.index.DocValues.Type#BYTES_VAR_SORTED} will be used.
 *
 * @lucene.experimental
 */
public class StrDocValuesFieldSource extends DocValuesFieldSource {

  private static class DVStrValues extends StrDocValues {

    private final Bits liveDocs;
    private final DocValues.Source source;

    public DVStrValues(ValueSource vs, DocValues.Source source, Bits liveDocs) {
      super(vs);
      this.liveDocs = liveDocs;
      this.source = source;
    }

    @Override
    public boolean exists(int doc) {
      return liveDocs == null || liveDocs.get(doc);
    }

    @Override
    public boolean bytesVal(int doc, BytesRef target) {
      source.getBytes(doc, target);
      return true;
    }

    @Override
    public String strVal(int doc) {
      BytesRef utf8Bytes = new BytesRef();
      source.getBytes(doc, utf8Bytes);
      return utf8Bytes.utf8ToString();
    }
  }

  /**
   * @param fieldName the name of the {@link DocValues} field
   * @param direct    whether or not to use a direct {@link org.apache.lucene.index.DocValues.Source}
   */
  public StrDocValuesFieldSource(String fieldName, boolean direct) {
    super(fieldName, direct);
  }

  @Override
  public FunctionValues getValues(@SuppressWarnings("rawtypes") Map context, AtomicReaderContext readerContext) throws IOException {
    final DocValues.Source source = getSource(readerContext.reader(), DocValues.Type.BYTES_VAR_SORTED);
    final Bits liveDocs = readerContext.reader().getLiveDocs();
    switch (source.getType()) {
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_STRAIGHT:
        return new DVStrValues(this, source, liveDocs);
      case BYTES_FIXED_SORTED:
      case BYTES_VAR_SORTED:
        final DocValues.SortedSource sortedSource = source.asSortedSource();
        if (sortedSource.hasPackedDocToOrd()) {
          final PackedInts.Reader docToOrd = sortedSource.getDocToOrd();
          return new DVStrValues(this, source, liveDocs) {

            @Override
            public int ordVal(int doc) {
              return (int) docToOrd.get(doc);
            }

            @Override
            public int numOrd() {
              return sortedSource.getValueCount();
            }

          };
        }
        return new DVStrValues(this, source, liveDocs) {

          @Override
          public int ordVal(int doc) {
            return sortedSource.ord(doc);
          }

          @Override
          public int numOrd() {
            return sortedSource.getValueCount();
          }

        };
      default:
        throw new IllegalStateException(getClass().getSimpleName() + " only works with binary types, not " + source.getType());
    }
  }

}
