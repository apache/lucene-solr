/**
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

package org.apache.solr.search.function;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.search.MutableValueInt;
import org.apache.solr.search.MutableValue;

import java.io.IOException;
import java.util.Map;


class ConstIntDocValues extends DocValues {
  final int ival;
  final float fval;
  final double dval;
  final long lval;
  final String sval;
  final ValueSource parent;

  ConstIntDocValues(int val, ValueSource parent) {
    ival = val;
    fval = val;
    dval = val;
    lval = val;
    sval = Integer.toString(val);
    this.parent = parent;
  }

  public float floatVal(int doc) {
    return fval;
  }
  public int intVal(int doc) {
    return ival;
  }
  public long longVal(int doc) {
    return lval;
  }
  public double doubleVal(int doc) {
    return dval;
  }
  public String strVal(int doc) {
    return sval;
  }
  public String toString(int doc) {
    return parent.description() + '=' + sval;
  }
}

class ConstDoubleDocValues extends DocValues {
  final int ival;
  final float fval;
  final double dval;
  final long lval;
  final String sval;
  final ValueSource parent;

  ConstDoubleDocValues(double val, ValueSource parent) {
    ival = (int)val;
    fval = (float)val;
    dval = val;
    lval = (long)val;
    sval = Double.toString(val);
    this.parent = parent;
  }

  public float floatVal(int doc) {
    return fval;
  }
  public int intVal(int doc) {
    return ival;
  }
  public long longVal(int doc) {
    return lval;
  }
  public double doubleVal(int doc) {
    return dval;
  }
  public String strVal(int doc) {
    return sval;
  }
  public String toString(int doc) {
    return parent.description() + '=' + sval;
  }
}

abstract class FloatDocValues extends DocValues {
  protected final ValueSource vs;

  public FloatDocValues(ValueSource vs) {
    this.vs = vs;    
  }

  @Override
  public byte byteVal(int doc) {
    return (byte)floatVal(doc);
  }

  @Override
  public short shortVal(int doc) {
    return (short)floatVal(doc);
  }

  @Override
  public abstract float floatVal(int doc);

  @Override
  public int intVal(int doc) {
    return (int)floatVal(doc);
  }

  @Override
  public long longVal(int doc) {
    return (long)floatVal(doc);
  }

  @Override
  public double doubleVal(int doc) {
    return (double)floatVal(doc);
  }

  @Override
  public String strVal(int doc) {
    return Float.toString(floatVal(doc));
  }

  @Override
  public String toString(int doc) {
    return vs.description() + '=' + strVal(doc);
  }
}

abstract class IntDocValues extends DocValues {
  protected final ValueSource vs;

  public IntDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public byte byteVal(int doc) {
    return (byte)intVal(doc);
  }

  @Override
  public short shortVal(int doc) {
    return (short)intVal(doc);
  }

  @Override
  public float floatVal(int doc) {
    return (float)intVal(doc);
  }

  @Override
  public abstract int intVal(int doc);

  @Override
  public long longVal(int doc) {
    return (long)intVal(doc);
  }

  @Override
  public double doubleVal(int doc) {
    return (double)intVal(doc);
  }

  @Override
  public String strVal(int doc) {
    return Integer.toString(intVal(doc));
  }

  @Override
  public String toString(int doc) {
    return vs.description() + '=' + strVal(doc);
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueInt mval = new MutableValueInt();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.value = intVal(doc);
      }
    };
  }
}


/**
 * <code>DocFreqValueSource</code> returns the number of documents containing the term.
 * @lucene.internal
 */
public class DocFreqValueSource extends ValueSource {
  protected String field;
  protected String indexedField;
  protected String val;
  protected BytesRef indexedBytes;

  public DocFreqValueSource(String field, String val, String indexedField, BytesRef indexedBytes) {
    this.field = field;
    this.val = val;
    this.indexedField = indexedField;
    this.indexedBytes = indexedBytes;
  }

  public String name() {
    return "docfreq";
  }

  @Override
  public String description() {
    return name() + '(' + field + ',' + val + ')';
  }

  @Override
  public DocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    IndexSearcher searcher = (IndexSearcher)context.get("searcher");
    int docfreq = searcher.docFreq(new Term(indexedField, indexedBytes));
    return new ConstIntDocValues(docfreq, this);
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    context.put("searcher",searcher);
  }

  public int hashCode() {
    return getClass().hashCode() + indexedField.hashCode()*29 + indexedBytes.hashCode();
  }

  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    DocFreqValueSource other = (DocFreqValueSource)o;
    return this.indexedField.equals(other.indexedField) && this.indexedBytes.equals(other.indexedBytes);
  }
}

