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
package org.apache.solr.search.function;

import java.io.IOException;
import java.util.function.IntFunction;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.solr.SolrTestCase;
import org.apache.solr.search.facet.FacetContext;
import org.apache.solr.search.facet.FacetMerger;
import org.apache.solr.search.facet.SimpleAggValueSource;
import org.apache.solr.search.facet.SlotAcc;
import org.junit.Test;

/** Tests that AggValueSource can be extended outside its package */
public class AggValueSourceTest extends SolrTestCase {

  @Test
  public void testCustomAgg() {
    // All we're really interested in testing here is that the custom agg compiles and can be created
    final CustomAggregate customAggregate = new CustomAggregate(new ConstValueSource(123.0f));
    final FacetMerger facetMerger = customAggregate.createFacetMerger(0.0D);
  }

  static class CustomAggregate extends SimpleAggValueSource {

    CustomAggregate(ValueSource vs) {
      super("customagg", vs);
    }

    @Override
    public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
      // check we can get access to the request and searcher, via the context
      if (fcontext.getRequest().getCore() != fcontext.getSearcher().getCore()) {
        throw new IllegalStateException("Searcher and request out of sync");
      }
      return new CustomSlotAcc(getArg(), fcontext, numSlots);
    }

    static class CustomSlotAcc extends SlotAcc.DoubleFuncSlotAcc {

      CustomSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
        super(values, fcontext, numSlots);
      }

      @Override
      public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
        result[slot] += values.doubleVal(doc);
      }

      @Override
      public Object getValue(int slot) {
        if (fcontext.isShard()) {
          // shard-specific logic here
        }
        return super.getValue(slot);
      }
    }

    @Override
    public FacetMerger createFacetMerger(Object prototype) {
      return new FacetMerger() {
        double total = 0.0D;

        @Override
        public void merge(Object facetResult, Context mcontext) {
          total += (Double)facetResult;
        }

        @Override
        public void finish(Context mcontext) { }

        @Override
        public Object getMergedResult() {
          return total;
        }
      };
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public String description() {
      return "customagg()";
    }
  }
}
