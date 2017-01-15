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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;

/**
 * Serves as base class for FunctionValues based on DocTermsIndex.
 * @lucene.internal
 */
public abstract class DocTermsIndexDocValues extends FunctionValues {
  protected final SortedDocValues termsIndex;
  protected final ValueSource vs;
  protected final MutableValueStr val = new MutableValueStr();
  protected final CharsRefBuilder spareChars = new CharsRefBuilder();
  private final String field;
  private int lastDocID;

  public DocTermsIndexDocValues(ValueSource vs, LeafReaderContext context, String field) throws IOException {
    this(field, vs, open(context, field));
  }
  
  protected DocTermsIndexDocValues(String field, ValueSource vs, SortedDocValues termsIndex) {
    this.field = field;
    this.vs = vs;
    this.termsIndex = termsIndex;
  }

  protected int getOrdForDoc(int doc) throws IOException {
    if (doc < lastDocID) {
      throw new IllegalArgumentException("docs were sent out-of-order: lastDocID=" + lastDocID + " vs docID=" + doc);
    }
    lastDocID = doc;
    int curDocID = termsIndex.docID();
    if (doc > curDocID) {
      curDocID = termsIndex.advance(doc);
    }
    if (doc == curDocID) {
      return termsIndex.ordValue();
    } else {
      return -1;
    }
  }

  protected abstract String toTerm(String readableValue);

  @Override
  public boolean exists(int doc) throws IOException {
    return getOrdForDoc(doc) >= 0;
  }

  @Override
  public int ordVal(int doc) throws IOException {
    return getOrdForDoc(doc);
  }

  @Override
  public int numOrd() {
    return termsIndex.getValueCount();
  }

  @Override
  public boolean bytesVal(int doc, BytesRefBuilder target) throws IOException {
    target.clear();
    if (getOrdForDoc(doc) == -1) {
      return false;
    } else {
      target.copyBytes(termsIndex.binaryValue());
      return true;
    }
  }

  @Override
  public String strVal(int doc) throws IOException {
    if (getOrdForDoc(doc) == -1) {
      return null;
    }
    final BytesRef term = termsIndex.binaryValue();
    spareChars.copyUTF8Bytes(term);
    return spareChars.toString();
  }

  @Override
  public boolean boolVal(int doc) throws IOException {
    return exists(doc);
  }

  @Override
  public abstract Object objectVal(int doc) throws IOException;  // force subclasses to override

  @Override
  public ValueSourceScorer getRangeScorer(LeafReaderContext readerContext, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) throws IOException {
    // TODO: are lowerVal and upperVal in indexed form or not?
    lowerVal = lowerVal == null ? null : toTerm(lowerVal);
    upperVal = upperVal == null ? null : toTerm(upperVal);

    int lower = Integer.MIN_VALUE;
    if (lowerVal != null) {
      lower = termsIndex.lookupTerm(new BytesRef(lowerVal));
      if (lower < 0) {
        lower = -lower-1;
      } else if (!includeLower) {
        lower++;
      }
    }

    int upper = Integer.MAX_VALUE;
    if (upperVal != null) {
      upper = termsIndex.lookupTerm(new BytesRef(upperVal));
      if (upper < 0) {
        upper = -upper-2;
      } else if (!includeUpper) {
        upper--;
      }
    }

    final int ll = lower;
    final int uu = upper;

    return new ValueSourceScorer(readerContext, this) {
      final SortedDocValues values = readerContext.reader().getSortedDocValues(field);
      private int lastDocID;
      
      @Override
      public boolean matches(int doc) throws IOException {
        if (doc < lastDocID) {
          throw new IllegalArgumentException("docs were sent out-of-order: lastDocID=" + lastDocID + " vs docID=" + doc);
        }
        if (doc > values.docID()) {
          values.advance(doc);
        }
        if (doc == values.docID()) {
          int ord = values.ordValue();
          return ord >= ll && ord <= uu;
        } else {
          return false;
        }
      }
    };
  }

  @Override
  public String toString(int doc) throws IOException {
    return vs.description() + '=' + strVal(doc);
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueStr mval = new MutableValueStr();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) throws IOException {
        int ord = getOrdForDoc(doc);
        mval.value.clear();
        mval.exists = ord >= 0;
        if (mval.exists) {
          mval.value.copyBytes(termsIndex.lookupOrd(ord));
        }
      }
    };
  }

  // TODO: why?
  static SortedDocValues open(LeafReaderContext context, String field) throws IOException {
    try {
      return DocValues.getSorted(context.reader(), field);
    } catch (RuntimeException e) {
      throw new DocTermsIndexException(field, e);
    }
  }
  
  /**
   * Custom Exception to be thrown when the DocTermsIndex for a field cannot be generated
   */
  public static final class DocTermsIndexException extends RuntimeException {

    public DocTermsIndexException(final String fieldName, final RuntimeException cause) {
      super("Can't initialize DocTermsIndex to generate (function) FunctionValues for field: " + fieldName, cause);
    }

  }


}
