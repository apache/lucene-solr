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

package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.function.IntFunction;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.function.FieldNameValueSource;

/**
 * {@link AggValueSource} to compute missing counts for given {@link ValueSource}
 */
public class MissingAgg extends SimpleAggValueSource {

  public MissingAgg(ValueSource vs) {
    super("missing", vs);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    ValueSource vs = getArg();

    if (vs instanceof FieldNameValueSource) {
      String field = ((FieldNameValueSource)vs).getFieldName();
      SchemaField sf = fcontext.qcontext.searcher().getSchema().getField(field);

      if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
        Query query = null;
        if (sf.hasDocValues()) {
          query = new DocValuesFieldExistsQuery(sf.getName());
        } else {
          query = sf.getType().getRangeQuery(null, sf, null, null, false, false);
        }
        vs = new QueryValueSource(query, 0.0f);
      } else {
        vs = sf.getType().getValueSource(sf, null);
      }
    }
    return new MissingSlotAcc(vs, fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetModule.FacetLongMerger();
  }

  class MissingSlotAcc extends SlotAcc.LongFuncSlotAcc {

    public MissingSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
      super(values, fcontext, numSlots, 0);
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      if (!values.exists(doc)) {
        result[slot]++;
      }
    }
  }

}
