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
import java.util.Date;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link ValueSource} for {@link DocValues} dates, backed by
 * {@link org.apache.lucene.index.DocValues.Type#FIXED_INTS_64}
 * or {@link org.apache.lucene.index.DocValues.Type#VAR_INTS}.
 * <p>
 * If the segment has no {@link DocValues}, the default
 * {@link org.apache.lucene.index.DocValues.Source} of type
 * {@link org.apache.lucene.index.DocValues.Type#FIXED_INTS_64} will be used.
 *
 * @lucene.experimental
 */
public class DateDocValuesFieldSource extends DocValuesFieldSource {

  private class DVDateValues extends LongDocValues {

    private final Bits liveDocs;
    private final DocValues.Source source;

    public DVDateValues(ValueSource vs, DocValues.Source source, Bits liveDocs) {
      super(vs);
      this.liveDocs = liveDocs;
      this.source = source;
    }

    @Override
    public boolean exists(int doc) {
      return liveDocs == null || liveDocs.get(doc);
    }

    @Override
    public long longVal(int doc) {
      return source.getInt(doc);
    }

    @Override
    public boolean bytesVal(int doc, BytesRef target) {
      source.getBytes(doc, target);
      return true;
    }

    @Override
    public Date objectVal(int doc) {
      return new Date(longVal(doc));
    }

    @Override
    public String strVal(int doc) {
      return dateToString(objectVal(doc));
    }

  }

  /**
   * @param fieldName the name of the {@link DocValues} field
   * @param direct    whether or not to use a direct {@link org.apache.lucene.index.DocValues.Source}
   */
  public DateDocValuesFieldSource(String fieldName, boolean direct) {
    super(fieldName, direct);
  }

  @Override
  public FunctionValues getValues(@SuppressWarnings("rawtypes") Map context, AtomicReaderContext readerContext) throws IOException {
    final DocValues.Source source = getSource(readerContext.reader(), DocValues.Type.FIXED_INTS_64);
    final Bits liveDocs = readerContext.reader().getLiveDocs();
    switch (source.getType()) {
      case FIXED_INTS_64:
      case VAR_INTS:
        if (source.hasArray() && source.getArray() instanceof long[]) {
          final long[] values = (long[]) source.getArray();
          return new DVDateValues(this, source, liveDocs) {

            @Override
            public long longVal(int doc) {
              return values[doc];
            }

          };
        }
        return new DVDateValues(this, source, liveDocs);
      default:
        throw new IllegalStateException(getClass().getSimpleName() + " only works with 64-bits integer types, not " + source.getType());
    }
  }

  /** Return the string representation of the provided {@link Date}.
   */
  protected String dateToString(Date date) {
    return date.toString();
  }

}
