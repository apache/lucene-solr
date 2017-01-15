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
package org.apache.lucene.queries.function.docvalues;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueDouble;

/**
 * Abstract {@link FunctionValues} implementation which supports retrieving double values.
 * Implementations can control how the double values are loaded through {@link #doubleVal(int)}}
 */
public abstract class DoubleDocValues extends FunctionValues {
  protected final ValueSource vs;

  public DoubleDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public byte byteVal(int doc) throws IOException {
    return (byte)doubleVal(doc);
  }

  @Override
  public short shortVal(int doc) throws IOException {
    return (short)doubleVal(doc);
  }

  @Override
  public float floatVal(int doc) throws IOException {
    return (float)doubleVal(doc);
  }

  @Override
  public int intVal(int doc) throws IOException {
    return (int)doubleVal(doc);
  }

  @Override
  public long longVal(int doc) throws IOException {
    return (long)doubleVal(doc);
  }

  @Override
  public boolean boolVal(int doc) throws IOException {
    return doubleVal(doc) != 0;
  }

  @Override
  public abstract double doubleVal(int doc) throws IOException;

  @Override
  public String strVal(int doc) throws IOException {
    return Double.toString(doubleVal(doc));
  }

  @Override
  public Object objectVal(int doc) throws IOException {
    return exists(doc) ? doubleVal(doc) : null;
  }

  @Override
  public String toString(int doc) throws IOException {
    return vs.description() + '=' + strVal(doc);
  }
  
  @Override
  public ValueSourceScorer getRangeScorer(LeafReaderContext readerContext, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
    double lower,upper;

    if (lowerVal==null) {
      lower = Double.NEGATIVE_INFINITY;
    } else {
      lower = Double.parseDouble(lowerVal);
    }

     if (upperVal==null) {
      upper = Double.POSITIVE_INFINITY;
    } else {
      upper = Double.parseDouble(upperVal);
    }

    final double l = lower;
    final double u = upper;


    if (includeLower && includeUpper) {
      return new ValueSourceScorer(readerContext, this) {
        @Override
        public boolean matches(int doc) throws IOException {
          if (!exists(doc)) return false;
          double docVal = doubleVal(doc);
          return docVal >= l && docVal <= u;
        }
      };
    }
    else if (includeLower && !includeUpper) {
      return new ValueSourceScorer(readerContext, this) {
        @Override
        public boolean matches(int doc) throws IOException {
          if (!exists(doc)) return false;
          double docVal = doubleVal(doc);
          return docVal >= l && docVal < u;
        }
      };
    }
    else if (!includeLower && includeUpper) {
      return new ValueSourceScorer(readerContext, this) {
        @Override
        public boolean matches(int doc) throws IOException {
          if (!exists(doc)) return false;
          double docVal = doubleVal(doc);
          return docVal > l && docVal <= u;
        }
      };
    }
    else {
      return new ValueSourceScorer(readerContext, this) {
        @Override
        public boolean matches(int doc) throws IOException {
          if (!exists(doc)) return false;
          double docVal = doubleVal(doc);
          return docVal > l && docVal < u;
        }
      };
    }
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueDouble mval = new MutableValueDouble();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) throws IOException {
        mval.value = doubleVal(doc);
        mval.exists = exists(doc);
      }
    };
  }

}
