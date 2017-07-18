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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrFieldSource;

public class MinMaxAgg extends SimpleAggValueSource {
  final int minmax; // a multiplier to reverse the normal order of compare if this is max instead of min (i.e. max will be -1)

  public MinMaxAgg(String minOrMax, ValueSource vs) {
    super(minOrMax, vs);
    minmax = "min".equals(name) ? 1 : -1;
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    ValueSource vs = getArg();

    if (vs instanceof StrFieldSource) {
      String field = ((StrFieldSource) vs).getField();
      SchemaField sf = fcontext.qcontext.searcher().getSchema().getField(field);
      if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
        if (sf.hasDocValues()) {
          // dv
        } else {
          // uif
        }
      } else {
        return new SingleValuedOrdAcc(fcontext, sf, numSlots);
      }
    }

    // numeric functions
    return new ValSlotAcc(vs, fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    if (prototype instanceof Number)
      return new NumericMerger();
    else if (prototype instanceof Comparable) {
      return new ComparableMerger();
    } else {
      throw new UnsupportedOperationException("min/max merge of " + prototype);
    }
  }

  // TODO: can this be replaced by ComparableMerger?
  private class NumericMerger extends FacetDoubleMerger {
    double val = Double.NaN;

    @Override
    public void merge(Object facetResult, Context mcontext) {
      double result = ((Number)facetResult).doubleValue();
      if (Double.compare(result, val)*minmax < 0 || Double.isNaN(val)) {
        val = result;
      }
    }

    @Override
    protected double getDouble() {
      return val;
    }
  }

  private class ComparableMerger extends FacetSortableMerger {
    Comparable val;
    @Override
    public void merge(Object facetResult, Context mcontext) {
      Comparable other = (Comparable)facetResult;
      if (val == null) {
        val = other;
      } else {
        if ( other.compareTo(val) * minmax < 0 ) {
          val = other;
        }
      }
    }

    @Override
    public Object getMergedResult() {
      return val;
    }

    @Override
    public int compareTo(FacetSortableMerger other, FacetRequest.SortDirection direction) {
      // NOTE: we don't use the minmax multiplier here because we still want natural ordering between slots (i.e. min(field) asc and max(field) asc) both sort "A" before "Z")
      return this.val.compareTo(((ComparableMerger)other).val);
    }
  }

  class ValSlotAcc extends DoubleFuncSlotAcc {
    public ValSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
      super(values, fcontext, numSlots, Double.NaN);
    }

    @Override
    public void collect(int doc, int slotNum) throws IOException {
      double val = values.doubleVal(doc);
      if (val == 0 && !values.exists(doc)) return; // depend on fact that non existing values return 0 for func query

      double currVal = result[slotNum];
      if (Double.compare(val, currVal) * minmax < 0 || Double.isNaN(currVal)) {
        result[slotNum] = val;
      }
    }
  }


  abstract class OrdAcc extends SlotAcc {
    final static int MISSING = -1;
    SchemaField field;
    int[] slotOrd;

    public OrdAcc(FacetContext fcontext, SchemaField field, int numSlots) throws IOException {
      super(fcontext);
      this.field = field;
      slotOrd = new int[numSlots];
      if (MISSING != 0) Arrays.fill(slotOrd, MISSING);
    }

    abstract BytesRef lookupOrd(int ord) throws IOException;

    @Override
    public int compare(int slotA, int slotB) {
      int a = slotOrd[slotA];
      int b = slotOrd[slotB];
      // NOTE: we don't use the minmax multiplier here because we still want natural ordering between slots (i.e. min(field) asc and max(field) asc) both sort "A" before "Z")
      return a - b;  // TODO: we probably want sort-missing-last functionality
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      int globOrd = slotOrd[slotNum];
      if (globOrd == MISSING) return null;
      BytesRef term = lookupOrd(globOrd);
      return field.getType().toObject(field, term);
    }

    @Override
    public void reset() throws IOException {
      Arrays.fill(slotOrd, MISSING);
    }

    @Override
    public void resize(Resizer resizer) {
      slotOrd = resizer.resize(slotOrd, MISSING);
    }
  }


  class SingleValuedOrdAcc extends OrdAcc {
    SortedDocValues topLevel;
    SortedDocValues[] subDvs;
    OrdinalMap ordMap;
    LongValues toGlobal;
    SortedDocValues subDv;

    public SingleValuedOrdAcc(FacetContext fcontext, SchemaField field, int numSlots) throws IOException {
      super(fcontext, field, numSlots);
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      topLevel = FieldUtil.getSortedDocValues(fcontext.qcontext, field, null);
      if (topLevel instanceof MultiDocValues.MultiSortedDocValues) {
        ordMap = ((MultiDocValues.MultiSortedDocValues)topLevel).mapping;
        subDvs = ((MultiDocValues.MultiSortedDocValues)topLevel).values;
      } else {
        ordMap = null;
        subDvs = null;
      }
    }

    @Override
    protected BytesRef lookupOrd(int ord) throws IOException {
      return topLevel.lookupOrd(ord);
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      if (topLevel == null) {
        reset();
      }
      super.setNextReader(readerContext);
      if (subDvs != null) {
        subDv = subDvs[readerContext.ord];
        toGlobal = ordMap.getGlobalOrds(readerContext.ord);
      } else {
        assert readerContext.ord==0 || topLevel.getValueCount() == 0;
        subDv = topLevel;
      }
    }

    @Override
    public void collect(int doc, int slotNum) throws IOException {
      if (doc > subDv.docID()) {
        subDv.advance(doc);
      }
      if (doc == subDv.docID()) {
        int segOrd = subDv.ordValue();
        int ord = toGlobal==null ? segOrd : (int)toGlobal.get(segOrd);
        if ((ord - slotOrd[slotNum]) * minmax < 0 || slotOrd[slotNum]==MISSING) {
          slotOrd[slotNum] = ord;
        }
      }
    }
  }


}
