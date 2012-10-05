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
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link ValueSource} for numeric {@link DocValues} types:<ul>
 * <li>{@link org.apache.lucene.index.DocValues.Type#FLOAT_32},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#FLOAT_64},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#FIXED_INTS_8},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#FIXED_INTS_16},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#FIXED_INTS_32},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#FIXED_INTS_64},</li>
 * <li>{@link org.apache.lucene.index.DocValues.Type#VAR_INTS}.</li></ul>
 * <p>
 * If the segment has no {@link DocValues}, the default
 * {@link org.apache.lucene.index.DocValues.Source} of type
 * {@link org.apache.lucene.index.DocValues.Type#FLOAT_64} will be used.
 *
 * @lucene.experimental
 */
public final class NumericDocValuesFieldSource extends DocValuesFieldSource {

  private static abstract class DVIntValues extends IntDocValues {

    private final Bits liveDocs;
    private final DocValues.Source source;

    public DVIntValues(ValueSource vs, DocValues.Source source, Bits liveDocs) {
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
    public int intVal(int doc) {
      return (int) source.getInt(doc);
    }
  }

  private static class DVLongValues extends LongDocValues {

    private final Bits liveDocs;
    private final DocValues.Source source;

    public DVLongValues(ValueSource vs, DocValues.Source source, Bits liveDocs) {
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
    public long longVal(int doc) {
      return source.getInt(doc);
    }
  }

  private static abstract class DVDoubleValues extends DoubleDocValues {

    private final Bits liveDocs;
    private final DocValues.Source source;

    public DVDoubleValues(ValueSource vs, DocValues.Source source, Bits liveDocs) {
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
    public double doubleVal(int doc) {
      return source.getFloat(doc);
    }
  }

  /**
   * @param fieldName the name of the {@link DocValues} field
   * @param direct    whether or not to use a direct {@link org.apache.lucene.index.DocValues.Source}
   */
  public NumericDocValuesFieldSource(String fieldName, boolean direct) {
    super(fieldName, direct);
  }

  @Override
  public FunctionValues getValues(@SuppressWarnings("rawtypes") Map context, AtomicReaderContext readerContext) throws IOException {
    final DocValues.Source source = getSource(readerContext.reader(), DocValues.Type.FLOAT_64);
    final Bits liveDocs = readerContext.reader().getLiveDocs();
    switch (source.getType()) {
      case FIXED_INTS_8:
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
      case VAR_INTS:
        if (source.hasArray()) {
          final Object valuesArr = source.getArray();
          if (valuesArr instanceof long[]) {
            final long[] values = (long[]) source.getArray();
            return new DVLongValues(this, source, liveDocs) {

              @Override
              public long longVal(int doc) {
                return values[doc];
              }

            };
          } else if (valuesArr instanceof int[]) {
            final int[] values = (int[]) source.getArray();
            return new DVIntValues(this, source, liveDocs) {

              @Override
              public int intVal(int doc) {
                return values[doc];
              }

            };
          } else if (valuesArr instanceof short[]) {
            final short[] values = (short[]) source.getArray();
            return new DVIntValues(this, source, liveDocs) {

              @Override
              public int intVal(int doc) {
                return values[doc];
              }

              @Override
              public Object objectVal(int doc) {
                return shortVal(doc);
              }

            };
          } else if (valuesArr instanceof byte[]) {
            final byte[] values = (byte[]) source.getArray();
            return new DVIntValues(this, source, liveDocs) {

              @Override
              public int intVal(int doc) {
                return values[doc];
              }

              @Override
              public Object objectVal(int doc) {
                return byteVal(doc);
              }

            };
          }
        }
        return new DVLongValues(this, source, liveDocs) {

          @Override
          public Object objectVal(int doc) {
            switch (source.getType()) {
              case FIXED_INTS_8:
                return byteVal(doc);
              case FIXED_INTS_16:
                return shortVal(doc);
              case FIXED_INTS_32:
                return intVal(doc);
              case FIXED_INTS_64:
              case VAR_INTS:
                return longVal(doc);
              default:
                throw new AssertionError();
            }
          }

        };
      case FLOAT_32:
      case FLOAT_64:
        if (source.hasArray()) {
          final Object valuesArr = source.getArray();
          if (valuesArr instanceof float[]) {
            final float[] values = (float[]) valuesArr;
            return new FloatDocValues(this) {

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
              public float floatVal(int doc) {
                return values[doc];
              }

            };
          } else if (valuesArr instanceof double[]) {
            final double[] values = (double[]) valuesArr;
            return new DVDoubleValues(this, source, liveDocs) {

              @Override
              public double doubleVal(int doc) {
                return values[doc];
              }

            };
          }
        }
        return new DVDoubleValues(this, source, liveDocs) {

          @Override
          public Object objectVal(int doc) {
            switch (source.getType()) {
              case FLOAT_32:
                return floatVal(doc);
              case FLOAT_64:
                return doubleVal(doc);
              default:
                throw new AssertionError();
            }
          }

        };
      default:
        throw new IllegalStateException(getClass().getSimpleName() + " only works with numeric types, not " + source.getType());
    }
  }

}
