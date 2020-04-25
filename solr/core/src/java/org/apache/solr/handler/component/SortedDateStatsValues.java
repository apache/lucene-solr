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
import java.util.Date;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.NamedList;

public class SortedDateStatsValues implements StatsValues {

  private final DateStatsValues dsv;
  private final String fieldName;
  private SortedNumericDocValues sndv;


  public SortedDateStatsValues(DateStatsValues dsv, StatsField field) {
    this.dsv = dsv;
    this.fieldName = field.getSchemaField().getName();
  }

  @Override
  public void accumulate(NamedList stv) {
    dsv.accumulate(stv);
  }

  @Override
  public void accumulate(int docId) throws IOException {
    if (!sndv.advanceExact(docId)) {
      missing();
    } else {
      for (int i = 0, count = sndv.docValueCount(); i < count; i++) {
        dsv.accumulate(new Date(sndv.nextValue()), 1);
      }
    }

  }

  @Override
  public void accumulate(BytesRef value, int count) {
    dsv.accumulate(value, count);
  }

  @Override
  public void missing() {
    dsv.missing();
  }

  @Override
  public void addMissing(int count) {
    dsv.addMissing(count);
  }

  @Override
  public void addFacet(String facetName, Map<String,StatsValues> facetValues) {
    dsv.addFacet(facetName, facetValues);
  }

  @Override
  public NamedList<?> getStatsValues() {
    return dsv.getStatsValues();
  }

  @Override
  public void setNextReader(LeafReaderContext ctx) throws IOException {
    sndv = DocValues.getSortedNumeric(ctx.reader(), fieldName);
    assert sndv != null;
  }
}
