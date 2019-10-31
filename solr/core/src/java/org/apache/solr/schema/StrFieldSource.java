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
package org.apache.solr.schema;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.DocTermsIndexDocValues;
import org.apache.lucene.queries.function.valuesource.FieldCacheSource;
import org.apache.lucene.search.SortField;

import java.io.IOException;
import java.util.Map;

public class StrFieldSource extends FieldCacheSource {

  private final SchemaField sf;

  public StrFieldSource(String field) {
    super(field);
    this.sf = null;
  }

  public StrFieldSource(SchemaField sf) {
    super(sf.name);
    this.sf = sf;
  }

  @Override
  public String description() {
    return "str(" + field + ')';
  }

  @Override
  public SortField getSortField(boolean reverse) {
    if (sf == null) {
      return super.getSortField(reverse);
    } else {
      return sf.getSortField(reverse);
    }
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    return new DocTermsIndexDocValues(this, readerContext, field) {

      @Override
      protected String toTerm(String readableValue) {
        return readableValue;
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
      public Object objectVal(int doc) throws IOException {
        return strVal(doc);
      }

      @Override
      public String toString(int doc) throws IOException {
        return description() + '=' + strVal(doc);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof StrFieldSource
            && super.equals(o);
  }

  private static int hcode = StrFieldSource.class.hashCode();
  @Override
  public int hashCode() {
    return hcode + super.hashCode();
  };
}
