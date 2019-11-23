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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.LongIterator;
import org.apache.solr.util.LongSet;

public class UniqueAgg extends StrAggValueSource {
  public static String UNIQUE = "unique";

  // internal constants used for aggregating values from multiple shards
  static String VALS = "vals";

  public UniqueAgg(String field) {
    super(UNIQUE, field);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    SchemaField sf = fcontext.qcontext.searcher().getSchema().getField(getArg());
    if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
      if (sf.getType().isPointField()) {
        return new SortedNumericAcc(fcontext, getArg(), numSlots);
      } else if (sf.hasDocValues()) {
        return new UniqueMultiDvSlotAcc(fcontext, sf, numSlots, null);
      } else {
        return new UniqueMultivaluedSlotAcc(fcontext, sf, numSlots, null);
      }
    } else {
      if (sf.getType().getNumberType() != null) {
        return new NumericAcc(fcontext, getArg(), numSlots);
      } else {
        return new UniqueSinglevaluedSlotAcc(fcontext, sf, numSlots, null);
      }
    }
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger();
  }

  private static class Merger extends FacetSortableMerger {
    long answer = -1;
    long sumUnique;
    Set<Object> values;
    long sumAdded;
    long shardsMissingSum;
    long shardsMissingMax;

    @Override
    public void merge(Object facetResult, Context mcontext) {
      SimpleOrderedMap map = (SimpleOrderedMap)facetResult;
      long unique = ((Number)map.get("unique")).longValue();
      sumUnique += unique;

      int valsListed = 0;
      List vals = (List) map.get("vals");
      if (vals != null) {
        if (values == null) {
          values = new HashSet<>(vals.size()*4);
        }
        values.addAll(vals);
        valsListed = vals.size();
        sumAdded += valsListed;
      }

      shardsMissingSum += unique - valsListed;
      shardsMissingMax = Math.max(shardsMissingMax, unique - valsListed);
      // TODO: somehow get & use the count in the bucket?
    }

    private long getLong() {
      if (answer >= 0) return answer;
      answer = values == null ? 0 : values.size();
      if (answer == 0) {
        // either a real "0", or no values returned from shards
        answer = shardsMissingSum;
        return answer;
      }

      double factor = ((double)values.size()) / sumAdded;  // what fraction of listed values were unique
      long estimate = (long)(shardsMissingSum * factor);
      answer = values.size() + estimate;
      return answer;
    }

    @Override
    public Object getMergedResult() {
      return getLong();
    }

    @Override
    public int compareTo(FacetSortableMerger other, FacetRequest.SortDirection direction) {
      return Long.compare( getLong(), ((Merger)other).getLong() );
    }
  }


  static abstract class BaseNumericAcc extends DocValuesAcc {
    LongSet[] sets;

    public BaseNumericAcc(FacetContext fcontext, String field, int numSlots) throws IOException {
      super(fcontext, fcontext.qcontext.searcher().getSchema().getField(field));
      sets = new LongSet[numSlots];
    }

    @Override
    public void reset() {
      sets = new LongSet[sets.length];
    }

    @Override
    public void resize(Resizer resizer) {
      sets = resizer.resize(sets, null);
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      LongSet set = sets[slot];
      if (set == null) {
        set = sets[slot] = new LongSet(16);
      }
      collectValues(doc, set);
    }

    protected abstract void collectValues(int doc, LongSet set) throws IOException;

    @Override
    public Object getValue(int slot) throws IOException {
      if (fcontext.isShard()) {
        return getShardValue(slot);
      }
      return getCardinality(slot);
    }

    private int getCardinality(int slot) {
      LongSet set = sets[slot];
      return set==null ? 0 : set.cardinality();
    }

    public Object getShardValue(int slot) throws IOException {
      LongSet set = sets[slot];
      int unique = getCardinality(slot);

      SimpleOrderedMap map = new SimpleOrderedMap();
      map.add("unique", unique);

      int maxExplicit=100;
      // TODO: make configurable
      // TODO: share values across buckets
      if (unique <= maxExplicit) {
        List lst = new ArrayList( Math.min(unique, maxExplicit) );
        if (set != null) {
          LongIterator iter = set.iterator();
          while (iter.hasNext()) {
            lst.add( iter.next() );
          }
        }
        map.add("vals", lst);
      }

      return map;
    }


    @Override
    public int compare(int slotA, int slotB) {
      return getCardinality(slotA) - getCardinality(slotB);
    }

  }

  static class NumericAcc extends BaseNumericAcc {
    NumericDocValues values;

    public NumericAcc(FacetContext fcontext, String field, int numSlots) throws IOException {
      super(fcontext, field, numSlots);
    }

    @Override
    protected DocIdSetIterator docIdSetIterator() {
      return values;
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      values = DocValues.getNumeric(readerContext.reader(),  sf.getName());
    }

    @Override
    protected void collectValues(int doc, LongSet set) throws IOException {
      set.add(values.longValue());
    }
  }

  static class SortedNumericAcc extends BaseNumericAcc {
    SortedNumericDocValues values;

    public SortedNumericAcc(FacetContext fcontext, String field, int numSlots) throws IOException {
      super(fcontext, field, numSlots);
    }

    @Override
    protected DocIdSetIterator docIdSetIterator() {
      return values;
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      values = DocValues.getSortedNumeric(readerContext.reader(),  sf.getName());
    }

    @Override
    protected void collectValues(int doc, LongSet set) throws IOException {
      for (int i = 0, count = values.docValueCount(); i < count; i++) {
        // duplicates may be produced for a single doc, but won't matter here.
        set.add(values.nextValue());
      }
    }
  }


}
