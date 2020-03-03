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
import java.util.Arrays;
import java.util.function.IntFunction;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.function.FieldNameValueSource;

/**
 * {@link AggValueSource} to count values for given {@link ValueSource}
 */
public class CountValsAgg extends SimpleAggValueSource {

  public CountValsAgg(ValueSource vs) {
    super("countvals", vs);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    ValueSource vs = getArg();
    if (vs instanceof FieldNameValueSource) {
      String field = ((FieldNameValueSource)vs).getFieldName();
      SchemaField sf = fcontext.qcontext.searcher().getSchema().getField(field);
      if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
        if (sf.hasDocValues()) {
          if (sf.getType().isPointField()) {
            return new CountSortedNumericDVAcc(fcontext, sf, numSlots);
          }
          return new CountSortedSetDVAcc(fcontext, sf, numSlots);
        }
        if (sf.getType().isPointField()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "'countvals' aggregation not supported for PointField without docValues");
        }
        return new CountMultiValuedAcc(fcontext, sf, numSlots);
      } else {
        vs = sf.getType().getValueSource(sf, null);
      }
    }
    return new CountValSlotAcc(vs, fcontext, numSlots);
  }


  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetLongMerger();
  }

  class CountValSlotAcc extends LongFuncSlotAcc {

    public CountValSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
      super(values, fcontext, numSlots, 0);
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      if (values.exists(doc)) {
        result[slot]++;
      }
    }
  }

  class CountSortedNumericDVAcc extends LongSortedNumericDVAcc {

    public CountSortedNumericDVAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      result[slot]+=values.docValueCount();
    }
  }

  class CountSortedSetDVAcc extends LongSortedSetDVAcc {

    public CountSortedSetDVAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      while (values.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
        result[slot]++;
      }
    }
  }

  class CountMultiValuedAcc extends UnInvertedFieldAcc {
    private int currentSlot;
    long[] result;

    public CountMultiValuedAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots);
      result = new long[numSlots];
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      this.currentSlot = slot;
      docToTerm.getBigTerms(doc + currentDocBase, this);
      docToTerm.getSmallTerms(doc + currentDocBase, this);
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Long.compare(result[slotA], result[slotB]);
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      return result[slotNum];
    }

    @Override
    public void reset() throws IOException {
      Arrays.fill(result, 0);
    }

    @Override
    public void resize(Resizer resizer) {
      resizer.resize(result, 0);
    }

    @Override
    public void call(int termNum) {
      result[currentSlot]++;
    }
  }
}
