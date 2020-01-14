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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.function.FieldNameValueSource;


public class AvgAgg extends SimpleAggValueSource {
  public AvgAgg(ValueSource vs) {
    super("avg", vs);
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
            return new AvgSortedNumericAcc(fcontext, sf, numSlots);
          }
          return new AvgSortedSetAcc(fcontext, sf, numSlots);
        }
        if (sf.getType().isPointField()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              name() + " aggregation not supported for PointField w/o docValues");
        }
        return new AvgUnInvertedFieldAcc(fcontext, sf, numSlots);
      }
      vs = sf.getType().getValueSource(sf, null);
    }
    return new AvgSlotAcc(vs, fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger();
  }

  private static class Merger extends FacetDoubleMerger {
    long num;
    double sum;

    @Override
    public void merge(Object facetResult, Context mcontext1) {
      List<Number> numberList = (List<Number>) facetResult;
      num += numberList.get(0).longValue();
      sum += numberList.get(1).doubleValue();
    }

    @Override
    protected double getDouble() {
      // TODO: is it worth to try and cache?
      return AggUtil.avg(sum, num);
    }
  }

  class AvgSortedNumericAcc extends DoubleSortedNumericDVAcc {
    int[] counts;

    public AvgSortedNumericAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
      this.counts = new int[numSlots];
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      for (int i = 0, count = values.docValueCount(); i < count; i++) {
        result[slot]+=getDouble(values.nextValue());
        counts[slot]++;
      }
    }

    private double avg(int slot) {
      return AggUtil.avg(result[slot], counts[slot]); // calc once and cache in result?
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(avg(slotA), avg(slotB));
    }

    @Override
    public Object getValue(int slot) {
      if (fcontext.isShard()) {
        ArrayList lst = new ArrayList(2);
        lst.add(counts[slot]);
        lst.add(result[slot]);
        return lst;
      } else {
        return avg(slot);
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      Arrays.fill(counts, 0);
    }

    @Override
    public void resize(Resizer resizer) {
      super.resize(resizer);
      resizer.resize(counts, 0);
    }
  }

  class AvgSortedSetAcc extends DoubleSortedSetDVAcc {
    int[] counts;

    public AvgSortedSetAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
      this.counts = new int[numSlots];
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      long ord;
      while ((ord = values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        BytesRef term = values.lookupOrd(ord);
        Object obj = sf.getType().toObject(sf, term);
        double val = obj instanceof Date ? ((Date)obj).getTime(): ((Number)obj).doubleValue();
        result[slot] += val;
        counts[slot]++;
      }
    }

    private double avg(int slot) {
      return AggUtil.avg(result[slot], counts[slot]);
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(avg(slotA), avg(slotB));
    }

    @Override
    public Object getValue(int slot) {
      if (fcontext.isShard()) {
        ArrayList lst = new ArrayList(2);
        lst.add(counts[slot]);
        lst.add(result[slot]);
        return lst;
      } else {
        return avg(slot);
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      Arrays.fill(counts, 0);
    }

    @Override
    public void resize(Resizer resizer) {
      super.resize(resizer);
      resizer.resize(counts, 0);
    }
  }

  class AvgUnInvertedFieldAcc extends DoubleUnInvertedFieldAcc {
    int[] counts;

    public AvgUnInvertedFieldAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
      this.counts = new int[numSlots];
    }

    @Override
    public void call(int termNum) {
      try {
        BytesRef term = docToTerm.lookupOrd(termNum);
        Object obj = sf.getType().toObject(sf, term);
        double val = obj instanceof Date? ((Date)obj).getTime(): ((Number)obj).doubleValue();
        result[currentSlot] += val;
        counts[currentSlot]++;
      } catch (IOException e) {
        // find a better way to do it
        throw new UncheckedIOException(e);
      }
    }

    private double avg(int slot) {
      return AggUtil.avg(result[slot], counts[slot]);
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(avg(slotA), avg(slotB));
    }

    @Override
    public Object getValue(int slot) {
      if (fcontext.isShard()) {
        ArrayList lst = new ArrayList(2);
        lst.add(counts[slot]);
        lst.add(result[slot]);
        return lst;
      } else {
        return avg(slot);
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      Arrays.fill(counts, 0);
    }

    @Override
    public void resize(Resizer resizer) {
      super.resize(resizer);
      resizer.resize(counts, 0);
    }
  }
}
