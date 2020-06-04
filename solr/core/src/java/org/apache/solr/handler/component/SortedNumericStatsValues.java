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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.NumberType;

public class SortedNumericStatsValues implements StatsValues {
  
  private final NumericStatsValues nsv;
  private final String fieldName;
  private final NumberType numberType;
  private SortedNumericDocValues sndv;
  
  
  public SortedNumericStatsValues(NumericStatsValues nsv, StatsField field) {
    this.nsv = nsv;
    this.fieldName = field.getSchemaField().getName();
    this.numberType = field.getSchemaField().getType().getNumberType();
  }

  @Override
  public void accumulate(@SuppressWarnings({"rawtypes"})NamedList stv) {
    nsv.accumulate(stv);
  }
  
  @Override
  public void accumulate(int docId) throws IOException {
    if (!sndv.advanceExact(docId)) {
      missing();
    } else {
      for (int i = 0, count = sndv.docValueCount(); i < count; i++) {
        nsv.accumulate(toCorrectType(sndv.nextValue()), 1);
      }
    }
    
  }

  private Number toCorrectType(long value) {
    switch (numberType) {
      case INTEGER:
      case LONG:
        return value;
      case FLOAT:
        return NumericUtils.sortableIntToFloat((int)value);
      case DOUBLE:
        return NumericUtils.sortableLongToDouble(value);
      default:
        throw new AssertionError("Unsupported number type");
    }
  }

  @Override
  public void accumulate(BytesRef value, int count) {
    nsv.accumulate(value, count);
  }

  @Override
  public void missing() {
    nsv.missing();
  }

  @Override
  public void addMissing(int count) {
    nsv.addMissing(count);
  }

  @Override
  public void addFacet(String facetName, Map<String,StatsValues> facetValues) {
    nsv.addFacet(facetName, facetValues);
  }

  @Override
  public NamedList<?> getStatsValues() {
    return nsv.getStatsValues();
  }

  @Override
  public void setNextReader(LeafReaderContext ctx) throws IOException {
    sndv = DocValues.getSortedNumeric(ctx.reader(), fieldName);
    assert sndv != null;
  }

}
