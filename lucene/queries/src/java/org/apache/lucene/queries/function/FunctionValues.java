package org.apache.lucene.queries.function;

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

import org.apache.lucene.search.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
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

  public byte byteVal(int doc) { throw new UnsupportedOperationException(); }
  public short shortVal(int doc) { throw new UnsupportedOperationException(); }

  public float floatVal(int doc) { throw new UnsupportedOperationException(); }
  public int intVal(int doc) { throw new UnsupportedOperationException(); }
  public long longVal(int doc) { throw new UnsupportedOperationException(); }
  public double doubleVal(int doc) { throw new UnsupportedOperationException(); }
  // TODO: should we make a termVal, returns BytesRef?
  public String strVal(int doc) { throw new UnsupportedOperationException(); }

  public boolean boolVal(int doc) {
    return intVal(doc) != 0;
  }

  /** returns the bytes representation of the string val - TODO: should this return the indexed raw bytes not? */
  public boolean bytesVal(int doc, BytesRef target) {
    String s = strVal(doc);
    if (s==null) {
      target.length = 0;
      return false;
    }
    target.copyChars(s);
    return true;
  }

  /** Native Java Object representation of the value */
  public Object objectVal(int doc) {
    // most FunctionValues are functions, so by default return a Float()
    return floatVal(doc);
  }

  /** Returns true if there is a value for this document */
  public boolean exists(int doc) {
    return true;
  }

  /**
   * @param doc The doc to retrieve to sort ordinal for
   * @return the sort ordinal for the specified doc
   * TODO: Maybe we can just use intVal for this...
   */
  public int ordVal(int doc) { throw new UnsupportedOperationException(); }

  /**
   * @return the number of unique sort ordinals this instance has
   */
  public int numOrd() { throw new UnsupportedOperationException(); }
  public abstract String toString(int doc);

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
    public abstract void fillValue(int doc);
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
      public void fillValue(int doc) {
        mval.value = floatVal(doc);
      }
    };
  }

  //For Functions that can work with multiple values from the same document.  This does not apply to all functions
  public void byteVal(int doc, byte [] vals) { throw new UnsupportedOperationException(); }
  public void shortVal(int doc, short [] vals) { throw new UnsupportedOperationException(); }

  public void floatVal(int doc, float [] vals) { throw new UnsupportedOperationException(); }
  public void intVal(int doc, int [] vals) { throw new UnsupportedOperationException(); }
  public void longVal(int doc, long [] vals) { throw new UnsupportedOperationException(); }
  public void doubleVal(int doc, double [] vals) { throw new UnsupportedOperationException(); }

  // TODO: should we make a termVal, fills BytesRef[]?
  public void strVal(int doc, String [] vals) { throw new UnsupportedOperationException(); }

  public Explanation explain(int doc) {
    return new Explanation(floatVal(doc), toString(doc));
  }

  public ValueSourceScorer getScorer(IndexReader reader) {
    return new ValueSourceScorer(reader, this);
  }

  // A RangeValueSource can't easily be a ValueSource that takes another ValueSource
  // because it needs different behavior depending on the type of fields.  There is also
  // a setup cost - parsing and normalizing params, and doing a binary search on the StringIndex.
  // TODO: change "reader" to AtomicReaderContext
  public ValueSourceScorer getRangeScorer(IndexReader reader, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
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
      return new ValueSourceScorer(reader, this) {
        @Override
        public boolean matchesValue(int doc) {
          float docVal = floatVal(doc);
          return docVal >= l && docVal <= u;
        }
      };
    }
    else if (includeLower && !includeUpper) {
       return new ValueSourceScorer(reader, this) {
        @Override
        public boolean matchesValue(int doc) {
          float docVal = floatVal(doc);
          return docVal >= l && docVal < u;
        }
      };
    }
    else if (!includeLower && includeUpper) {
       return new ValueSourceScorer(reader, this) {
        @Override
        public boolean matchesValue(int doc) {
          float docVal = floatVal(doc);
          return docVal > l && docVal <= u;
        }
      };
    }
    else {
       return new ValueSourceScorer(reader, this) {
        @Override
        public boolean matchesValue(int doc) {
          float docVal = floatVal(doc);
          return docVal > l && docVal < u;
        }
      };
    }
  }
}



