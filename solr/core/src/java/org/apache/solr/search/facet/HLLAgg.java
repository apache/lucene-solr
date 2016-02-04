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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.util.hll.HLL;
import org.apache.solr.util.hll.HLLType;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;

public class HLLAgg extends StrAggValueSource {
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
      if (sf.hasDocValues()) {
        return new UniqueMultiDvSlotAcc(fcontext, getArg(), numSlots, fcontext.isShard() ? factory : null);
      } else {
        return new UniqueMultivaluedSlotAcc(fcontext, getArg(), numSlots, fcontext.isShard() ? factory : null);
      }
    } else {
      if (sf.getType().getNumericType() != null) {
        // always use hll here since we don't know how many values there are?
        return new NumericAcc(fcontext, getArg(), numSlots);
      } else {
        return new UniqueSinglevaluedSlotAcc(fcontext, getArg(), numSlots, fcontext.isShard() ? factory : null);
      }
    }
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger();
  }

  private static class Merger extends FacetSortableMerger {
    HLL aggregate = null;
    long answer = -1;

    @Override
    public void merge(Object facetResult, Context mcontext) {
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
        answer = aggregate.cardinality();
      }
      return answer;
    }

    @Override
    public Object getMergedResult() {
      return getLong();
    }

    @Override
    public int compareTo(FacetSortableMerger other, FacetField.SortDirection direction) {
      return Long.compare( getLong(), ((Merger)other).getLong() );
    }
  }


  // TODO: hybrid model for non-distrib numbers?
  // todo - better efficiency for sorting?

  class NumericAcc extends SlotAcc {
    SchemaField sf;
    HLL[] sets;
    NumericDocValues values;
    Bits exists;

    public NumericAcc(FacetContext fcontext, String field, int numSlots) throws IOException {
      super(fcontext);
      sf = fcontext.searcher.getSchema().getField(field);
      sets = new HLL[numSlots];
    }

    @Override
    public void reset() {
      sets = new HLL[sets.length];
    }

    @Override
    public void resize(Resizer resizer) {
      resizer.resize(sets, null);
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      values = DocValues.getNumeric(readerContext.reader(),  sf.getName());
      exists = DocValues.getDocsWithField(readerContext.reader(), sf.getName());
    }

    @Override
    public void collect(int doc, int slot) throws IOException {
      long val = values.get(doc);
      if (val == 0 && !exists.get(doc)) {
        return;
      }

      long hash = Hash.fmix64(val);

      HLL hll = sets[slot];
      if (hll == null) {
        hll = sets[slot] = factory.getHLL();
      }
      hll.addRaw(hash);
    }

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

    public Object getShardValue(int slot) throws IOException {
      HLL hll = sets[slot];
      if (hll == null) return null;
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


}
