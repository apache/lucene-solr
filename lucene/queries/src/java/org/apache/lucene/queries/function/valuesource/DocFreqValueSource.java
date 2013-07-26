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

package org.apache.lucene.queries.function.valuesource;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Map;


class ConstIntDocValues extends IntDocValues {
  final int ival;
  final float fval;
  final double dval;
  final long lval;
  final String sval;
  final ValueSource parent;

  ConstIntDocValues(int val, ValueSource parent) {
    super(parent);
    ival = val;
    fval = val;
    dval = val;
    lval = val;
    sval = Integer.toString(val);
    this.parent = parent;
  }

  @Override
  public float floatVal(int doc) {
    return fval;
  }
  @Override
  public int intVal(int doc) {
    return ival;
  }
  @Override
  public long longVal(int doc) {
    return lval;
  }
  @Override
  public double doubleVal(int doc) {
    return dval;
  }
  @Override
  public String strVal(int doc) {
    return sval;
  }
  @Override
  public String toString(int doc) {
    return parent.description() + '=' + sval;
  }
}

class ConstDoubleDocValues extends DoubleDocValues {
  final int ival;
  final float fval;
  final double dval;
  final long lval;
  final String sval;
  final ValueSource parent;

  ConstDoubleDocValues(double val, ValueSource parent) {
    super(parent);
    ival = (int)val;
    fval = (float)val;
    dval = val;
    lval = (long)val;
    sval = Double.toString(val);
    this.parent = parent;
  }

  @Override
  public float floatVal(int doc) {
    return fval;
  }
  @Override
  public int intVal(int doc) {
    return ival;
  }
  @Override
  public long longVal(int doc) {
    return lval;
  }
  @Override
  public double doubleVal(int doc) {
    return dval;
  }
  @Override
  public String strVal(int doc) {
    return sval;
  }
  @Override
  public String toString(int doc) {
    return parent.description() + '=' + sval;
  }
}


/**
 * <code>DocFreqValueSource</code> returns the number of documents containing the term.
 * @lucene.internal
 */
public class DocFreqValueSource extends ValueSource {
  protected final String field;
  protected final String indexedField;
  protected final String val;
  protected final BytesRef indexedBytes;

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
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    IndexSearcher searcher = (IndexSearcher)context.get("searcher");
    int docfreq = searcher.getIndexReader().docFreq(new Term(indexedField, indexedBytes));
    return new ConstIntDocValues(docfreq, this);
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    context.put("searcher",searcher);
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() + indexedField.hashCode()*29 + indexedBytes.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    DocFreqValueSource other = (DocFreqValueSource)o;
    return this.indexedField.equals(other.indexedField) && this.indexedBytes.equals(other.indexedBytes);
  }
}

