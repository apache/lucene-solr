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
package org.apache.lucene.queries.function;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueFloat;

/**
 * Represents field values as different types.
 * Normally created via a {@link ValueSource} for a particular field and reader.
 *
 *
 */

// FunctionValues is distinct from ValueSource because
// there needs to be an object created at query evaluation time that
// is not referenced by the query itself because:
// - Query objects should be MT safe
// - For caching, Query objects are often used as keys... you don't
//   want the Query carrying around big objects
public abstract class FunctionValues {

  public byte byteVal(int doc) throws IOException { throw new UnsupportedOperationException(); }
  public short shortVal(int doc) throws IOException { throw new UnsupportedOperationException(); }

  public float floatVal(int doc) throws IOException { throw new UnsupportedOperationException(); }
  public int intVal(int doc) throws IOException { throw new UnsupportedOperationException(); }
  public long longVal(int doc) throws IOException { throw new UnsupportedOperationException(); }
  public double doubleVal(int doc) throws IOException { throw new UnsupportedOperationException(); }
  // TODO: should we make a termVal, returns BytesRef?
  public String strVal(int doc) throws IOException { throw new UnsupportedOperationException(); }

  public boolean boolVal(int doc) throws IOException {
    return intVal(doc) != 0;
  }

  /** returns the bytes representation of the string val - TODO: should this return the indexed raw bytes not? */
  public boolean bytesVal(int doc, BytesRefBuilder target) throws IOException {
    String s = strVal(doc);
    if (s==null) {
      target.clear();
      return false;
    }
    target.copyChars(s);
    return true;
  }

  /** Native Java Object representation of the value */
  public Object objectVal(int doc) throws IOException {
    // most FunctionValues are functions, so by default return a Float()
    return floatVal(doc);
  }

  /** Returns true if there is a value for this document */
  public boolean exists(int doc) throws IOException {
    return true;
  }

  /**
   * @param doc The doc to retrieve to sort ordinal for
   * @return the sort ordinal for the specified doc
   * TODO: Maybe we can just use intVal for this...
   */
  public int ordVal(int doc) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the number of unique sort ordinals this instance has
   */
  public int numOrd() { throw new UnsupportedOperationException(); }
  public abstract String toString(int doc) throws IOException;

  /**
   * Abstraction of the logic required to fill the value of a specified doc into
   * a reusable {@link MutableValue}.  Implementations of {@link FunctionValues}
   * are encouraged to define their own implementations of ValueFiller if their
   * value is not a float.
   *
   * @lucene.experimental
   */
  public static abstract class ValueFiller {
    /** MutableValue will be reused across calls */
    public abstract MutableValue getValue();

    /** MutableValue will be reused across calls.  Returns true if the value exists. */
    public abstract void fillValue(int doc) throws IOException;
  }

  /** @lucene.experimental  */
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueFloat mval = new MutableValueFloat();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) throws IOException {
        mval.value = floatVal(doc);
      }
    };
  }

  //For Functions that can work with multiple values from the same document.  This does not apply to all functions
  public void byteVal(int doc, byte [] vals) throws IOException { throw new UnsupportedOperationException(); }
  public void shortVal(int doc, short [] vals) throws IOException { throw new UnsupportedOperationException(); }

  public void floatVal(int doc, float [] vals) throws IOException { throw new UnsupportedOperationException(); }
  public void intVal(int doc, int [] vals) throws IOException { throw new UnsupportedOperationException(); }
  public void longVal(int doc, long [] vals) throws IOException { throw new UnsupportedOperationException(); }
  public void doubleVal(int doc, double [] vals) throws IOException { throw new UnsupportedOperationException(); }

  // TODO: should we make a termVal, fills BytesRef[]?
  public void strVal(int doc, String [] vals) throws IOException { throw new UnsupportedOperationException(); }

  public Explanation explain(int doc) throws IOException {
    return Explanation.match(floatVal(doc), toString(doc));
  }

  /**
   * Yields a {@link Scorer} that matches all documents,
   * and that which produces scores equal to {@link #floatVal(int)}.
   */
  public ValueSourceScorer getScorer(Weight weight, LeafReaderContext readerContext) {
    return new ValueSourceScorer(weight, readerContext, this) {
      @Override
      public boolean matches(int doc) {
        return true;
      }
    };
  }

  /**
   * Yields a {@link Scorer} that matches documents with values between the specified range,
   * and that which produces scores equal to {@link #floatVal(int)}.
   */
  // A RangeValueSource can't easily be a ValueSource that takes another ValueSource
  // because it needs different behavior depending on the type of fields.  There is also
  // a setup cost - parsing and normalizing params, and doing a binary search on the StringIndex.
  // TODO: change "reader" to LeafReaderContext
  public ValueSourceScorer getRangeScorer(Weight weight, LeafReaderContext readerContext, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) throws IOException {
    float lower;
    float upper;

    if (lowerVal == null) {
      lower = Float.NEGATIVE_INFINITY;
    } else {
      lower = Float.parseFloat(lowerVal);
    }
    if (upperVal == null) {
      upper = Float.POSITIVE_INFINITY;
    } else {
      upper = Float.parseFloat(upperVal);
    }

    final float l = lower;
    final float u = upper;

    if (includeLower && includeUpper) {
      return new ValueSourceScorer(weight, readerContext, this) {
        @Override
        public boolean matches(int doc) throws IOException {
          if (!exists(doc)) return false;
          float docVal = floatVal(doc);
          return docVal >= l && docVal <= u;
        }
      };
    }
    else if (includeLower && !includeUpper) {
       return new ValueSourceScorer(weight, readerContext, this) {
        @Override
        public boolean matches(int doc) throws IOException {
          if (!exists(doc)) return false;
          float docVal = floatVal(doc);
          return docVal >= l && docVal < u;
        }
      };
    }
    else if (!includeLower && includeUpper) {
       return new ValueSourceScorer(weight, readerContext, this) {
        @Override
        public boolean matches(int doc) throws IOException {
          if (!exists(doc)) return false;
          float docVal = floatVal(doc);
          return docVal > l && docVal <= u;
        }
      };
    }
    else {
       return new ValueSourceScorer(weight, readerContext, this) {
        @Override
        public boolean matches(int doc) throws IOException {
          if (!exists(doc)) return false;
          float docVal = floatVal(doc);
          return docVal > l && docVal < u;
        }
      };
    }
  }
}



