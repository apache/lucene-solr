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
import java.util.List;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.function.FieldNameValueSource;


public class VarianceAgg extends SimpleAggValueSource {
  public VarianceAgg(ValueSource vs) {
    super("variance", vs);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    ValueSource vs = getArg();

    if (vs instanceof FieldNameValueSource) {
      String field = ((FieldNameValueSource) vs).getFieldName();
      SchemaField sf = fcontext.qcontext.searcher().getSchema().getField(field);
      if (sf.getType().getNumberType() == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            name() + " aggregation not supported for " + sf.getType().getTypeName());
      }
      if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
        if (sf.hasDocValues()) {
          if (sf.getType().isPointField()) {
            return new VarianceSortedNumericAcc(fcontext, sf, numSlots);
          }
          return new VarianceSortedSetAcc(fcontext, sf, numSlots);
        }
        if (sf.getType().isPointField()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              name() + " aggregation not supported for PointField w/o docValues");
        }
        return new VarianceUnInvertedFieldAcc(fcontext, sf, numSlots);
      }
      vs = sf.getType().getValueSource(sf, null);
    }
    return new SlotAcc.VarianceSlotAcc(vs, fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger();
  }

  private static class Merger extends FacetModule.FacetDoubleMerger {
    long count;
    double sumSq;
    double sum;
    
    @Override
    @SuppressWarnings("unchecked")
    public void merge(Object facetResult, Context mcontext1) {
      List<Number> numberList = (List<Number>)facetResult;
      this.count += numberList.get(0).longValue();
      this.sumSq += numberList.get(1).doubleValue();
      this.sum += numberList.get(2).doubleValue();
    }

    @Override
    public Object getMergedResult() {
      return this.getDouble();
    }
    
    @Override
    protected double getDouble() {
      return AggUtil.uncorrectedVariance(sumSq, sum, count);
    }    
  }

  class VarianceSortedNumericAcc extends DocValuesAcc.SDVSortedNumericAcc {

    public VarianceSortedNumericAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots);
    }

    @Override
    protected double computeVal(int slot) {
      return AggUtil.uncorrectedVariance(result[slot], sum[slot], counts[slot]); // calc once and cache in result?
    }
  }

  class VarianceSortedSetAcc extends DocValuesAcc.SDVSortedSetAcc {

    public VarianceSortedSetAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots);
    }

    @Override
    protected double computeVal(int slot) {
      return AggUtil.uncorrectedVariance(result[slot], sum[slot], counts[slot]); // calc once and cache in result?
    }
  }

  class VarianceUnInvertedFieldAcc extends UnInvertedFieldAcc.SDVUnInvertedFieldAcc {

    public VarianceUnInvertedFieldAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots);
    }

    @Override
    protected double computeVal(int slot) {
      return AggUtil.uncorrectedVariance(result[slot], sum[slot], counts[slot]); // calc once and cache in result?
    }
  }
}
