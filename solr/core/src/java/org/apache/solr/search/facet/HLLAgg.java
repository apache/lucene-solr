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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.hll.HLL;
import org.apache.solr.util.hll.HLLType;

public class HLLAgg extends StrAggValueSource {
  public static Integer NO_VALUES = 0;

  protected HLLFactory factory;

  public HLLAgg(String field) {
    super("hll", field);
    factory = new HLLFactory();
  }

  // factory for the hyper-log-log algorithm.
  // TODO: make stats component HllOptions inherit from this?
  public static class HLLFactory {
    int log2m = 13;
    int regwidth = 6;
    public HLL getHLL() {
      return new HLL(log2m, regwidth, -1 /* auto explict threshold */,
          false /* no sparse representation */, HLLType.EMPTY);
    }
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    SchemaField sf = fcontext.qcontext.searcher().getSchema().getField(getArg());
    if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
      if (sf.getType().isPointField()) {
        return new SortedNumericAcc(fcontext, getArg(), numSlots);
      } else if (sf.hasDocValues()) {
        return new UniqueMultiDvSlotAcc(fcontext, sf, numSlots, fcontext.isShard() ? factory : null);
      } else {
        return new UniqueMultivaluedSlotAcc(fcontext, sf, numSlots, fcontext.isShard() ? factory : null);
      }
    } else {
      if (sf.getType().getNumberType() != null) {
        // always use hll here since we don't know how many values there are?
        return new NumericAcc(fcontext, getArg(), numSlots);
      } else {
        return new UniqueSinglevaluedSlotAcc(fcontext, sf, numSlots, fcontext.isShard() ? factory : null);
      }
    }
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger();
  }

  private static class Merger extends FacetModule.FacetSortableMerger {
    HLL aggregate = null;
    long answer = -1; // -1 means unset

    @Override
    public void merge(Object facetResult, Context mcontext) {
      if (facetResult instanceof Number) {
        assert NO_VALUES.equals(facetResult);
        return;
      }



      @SuppressWarnings({"rawtypes"})
      SimpleOrderedMap map = (SimpleOrderedMap)facetResult;
      byte[] serialized = ((byte[])map.get("hll"));
      HLL subHLL = HLL.fromBytes(serialized);
      if (aggregate == null) {
        aggregate = subHLL;
      } else {
        aggregate.union(subHLL);
      }
    }

    private long getLong() {
      if (answer < 0) {
        answer = aggregate == null ? 0 : aggregate.cardinality();
      }
      return answer;
    }

    @Override
    public Object getMergedResult() {
      return getLong();
    }

    @Override
    public int compareTo(FacetModule.FacetSortableMerger other, FacetRequest.SortDirection direction) {
      return Long.compare( getLong(), ((Merger)other).getLong() );
    }
  }


  // TODO: hybrid model for non-distrib numbers?
  // todo - better efficiency for sorting?

  abstract class BaseNumericAcc extends DocValuesAcc {
    HLL[] sets;

    public BaseNumericAcc(FacetContext fcontext, String field, int numSlots) throws IOException {
      super(fcontext, fcontext.qcontext.searcher().getSchema().getField(field));
      sets = new HLL[numSlots];
    }

    @Override
    public void reset() {
      sets = new HLL[sets.length];
    }

    @Override
    public void resize(Resizer resizer) {
      sets = resizer.resize(sets, null);
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      HLL hll = sets[slot];
      if (hll == null) {
        hll = sets[slot] = factory.getHLL();
      }
      collectValues(doc, hll);
    }

    protected abstract void collectValues(int doc, HLL hll) throws IOException;

    @Override
    public Object getValue(int slot) throws IOException {
      if (fcontext.isShard()) {
        return getShardValue(slot);
      }
      return getCardinality(slot);
    }

    private int getCardinality(int slot) {
      HLL set = sets[slot];
      return set==null ? 0 : (int)set.cardinality();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object getShardValue(int slot) throws IOException {
      HLL hll = sets[slot];
      if (hll == null) return NO_VALUES;
      SimpleOrderedMap map = new SimpleOrderedMap();
      map.add("hll", hll.toBytes());
      // optionally use explicit values
      return map;
    }

    @Override
    public int compare(int slotA, int slotB) {
      return getCardinality(slotA) - getCardinality(slotB);
    }

  }

  class NumericAcc extends BaseNumericAcc {
    NumericDocValues values;

    public NumericAcc(FacetContext fcontext, String field, int numSlots) throws IOException {
      super(fcontext, field, numSlots);
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      values = DocValues.getNumeric(readerContext.reader(),  sf.getName());
    }

    protected DocIdSetIterator docIdSetIterator() {
      return values;
    }

    @Override
    protected boolean advanceExact(int doc) throws IOException {
      return values.advanceExact(doc);
    }

    @Override
    protected void collectValues(int doc, HLL hll) throws IOException {
      long val = values.longValue();
      long hash = Hash.fmix64(val);
      hll.addRaw(hash);
    }
  }

  class SortedNumericAcc extends BaseNumericAcc {
    SortedNumericDocValues values;

    public SortedNumericAcc(FacetContext fcontext, String field, int numSlots) throws IOException {
      super(fcontext, field, numSlots);
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      values = DocValues.getSortedNumeric(readerContext.reader(),  sf.getName());
    }

    protected DocIdSetIterator docIdSetIterator() {
      return values;
    }

    @Override
    protected boolean advanceExact(int doc) throws IOException {
      return values.advanceExact(doc);
    }

    @Override
    protected void collectValues(int doc, HLL hll) throws IOException {
      for (int i = 0, count = values.docValueCount(); i < count; i++) {
        // duplicates may be produced for a single doc, but won't matter here.
        long val = values.nextValue();
        long hash = Hash.fmix64(val);
        hll.addRaw(hash);
      }
    }
  }

}
