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
import java.util.Date;
import java.util.function.IntFunction;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrFieldSource;
import org.apache.solr.search.function.FieldNameValueSource;

public class MinMaxAgg extends SimpleAggValueSource {
  final int minmax; // a multiplier to reverse the normal order of compare if this is max instead of min (i.e. max will be -1)

  public MinMaxAgg(String minOrMax, ValueSource vs) {
    super(minOrMax, vs);
    minmax = "min".equals(name) ? 1 : -1;
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    ValueSource vs = getArg();

    SchemaField sf = null;

    if (vs instanceof FieldNameValueSource) {
      String field = ((FieldNameValueSource)vs).getFieldName();
      sf = fcontext.qcontext.searcher().getSchema().getField(field);

      if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
        if (sf.hasDocValues()) {
          if (sf.getType().isPointField()) {
            FieldType.MultiValueSelector choice = minmax == 1 ? FieldType.MultiValueSelector.MIN : FieldType.MultiValueSelector.MAX;
            vs = sf.getType().getSingleValueSource(choice, sf, null);
          } else {
            NumberType numberType = sf.getType().getNumberType();
            if (numberType != null && numberType != NumberType.DATE) {
              // TrieDate doesn't support selection of single value
              FieldType.MultiValueSelector choice = minmax == 1 ? FieldType.MultiValueSelector.MIN : FieldType.MultiValueSelector.MAX;
              vs = sf.getType().getSingleValueSource(choice, sf, null);
            } else {
              return new MinMaxSortedSetDVAcc(fcontext, sf, numSlots);
            }
          }
        } else {
          if (sf.getType().isPointField()) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "min/max aggregations can't be used on PointField w/o DocValues");
          }
          return new MinMaxUnInvertedFieldAcc(fcontext, sf, numSlots);
        }
      } else {
        vs = sf.getType().getValueSource(sf, null);
      }
    }

    if (vs instanceof StrFieldSource) {
      return new SingleValuedOrdAcc(fcontext, sf, numSlots);
    }

    // Since functions don't currently have types, we rely on the type of the field
    if (sf != null && sf.getType().getNumberType() != null) {
      switch (sf.getType().getNumberType()) {
        case FLOAT:
        case DOUBLE:
          return new DFuncAcc(vs, fcontext, numSlots);
        case INTEGER:
        case LONG:
          return new LFuncAcc(vs, fcontext, numSlots);
        case DATE:
          return new DateFuncAcc(vs, fcontext, numSlots);
      }
    }

    // numeric functions
    return new DFuncAcc(vs, fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    if (prototype instanceof Double)
      return new NumericMerger(); // still use NumericMerger to handle NaN?
    else if (prototype instanceof Comparable) {
      return new ComparableMerger();
    } else {
      throw new UnsupportedOperationException("min/max merge of " + prototype);
    }
  }

  // TODO: can this be replaced by ComparableMerger?
  private class NumericMerger extends FacetModule.FacetDoubleMerger {
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

  private class ComparableMerger extends FacetModule.FacetSortableMerger {
    @SuppressWarnings("rawtypes")
    Comparable val;
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
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
    @SuppressWarnings({"unchecked"})
    public int compareTo(FacetModule.FacetSortableMerger other, FacetRequest.SortDirection direction) {
      // NOTE: we don't use the minmax multiplier here because we still want natural ordering between slots (i.e. min(field) asc and max(field) asc) both sort "A" before "Z")
      return this.val.compareTo(((ComparableMerger)other).val);
    }
  }

  class MinMaxUnInvertedFieldAcc extends UnInvertedFieldAcc {
    final static int MISSING = -1;
    private int currentSlot;
    int[] result;

    public MinMaxUnInvertedFieldAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots);
      result = new int[numSlots];
      Arrays.fill(result, MISSING);
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      this.currentSlot = slot;
      docToTerm.getBigTerms(doc + currentDocBase, this);
      docToTerm.getSmallTerms(doc + currentDocBase, this);
    }

    @Override
    public int compare(int slotA, int slotB) {
      int a = result[slotA];
      int b = result[slotB];
      return a == MISSING ? -1: (b == MISSING? 1: Integer.compare(a, b));
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      int ord = result[slotNum];
      if (ord == MISSING) return null;
      BytesRef term = docToTerm.lookupOrd(ord);
      return getObject(term);
    }

    /**
     * Wrapper to convert stored format to external format.
     * <p>
     * This ensures consistent behavior like other accumulators where
     * long is returned for integer field types and double is returned for float field types
     * </p>
     */
    private Object getObject(BytesRef term) {
      Object obj = sf.getType().toObject(sf, term);
      NumberType type = sf.getType().getNumberType();
      if (type == null) {
        return obj;
      } else if (type == NumberType.INTEGER) {
        // this is to ensure consistent behavior with other accumulators
        // where long is returned for integer field types
        return ((Number)obj).longValue();
      } else if (type == NumberType.FLOAT) {
        return ((Number)obj).floatValue();
      }
      return obj;
    }

    @Override
    public void reset() throws IOException {
      Arrays.fill(result, MISSING);
    }

    @Override
    public void resize(Resizer resizer) {
      this.result = resizer.resize(result, MISSING);
    }

    @Override
    public void call(int termNum) {
      int currOrd = result[currentSlot];
      if (currOrd == MISSING || Integer.compare(termNum, currOrd) * minmax < 0) {
        result[currentSlot] = termNum;
      }
    }
  }

  class DFuncAcc extends SlotAcc.DoubleFuncSlotAcc {
    public DFuncAcc(ValueSource values, FacetContext fcontext, int numSlots) {
      super(values, fcontext, numSlots, Double.NaN);
    }

    @Override
    public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
      double val = values.doubleVal(doc);
      if (val == 0 && !values.exists(doc)) return; // depend on fact that non existing values return 0 for func query

      double currVal = result[slotNum];
      if (Double.compare(val, currVal) * minmax < 0 || Double.isNaN(currVal)) {
        result[slotNum] = val;
      }
    }

    @Override
    public Object getValue(int slot) {
      double val = result[slot];
      if (Double.isNaN(val)) {
        return null;
      } else {
        return val;
      }
    }
  }

  class LFuncAcc extends SlotAcc.LongFuncSlotAcc {
    FixedBitSet exists;
    public LFuncAcc(ValueSource values, FacetContext fcontext, int numSlots) {
      super(values, fcontext, numSlots, 0);
      exists = new FixedBitSet(numSlots);
    }

    @Override
    public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
      long val = values.longVal(doc);
      if (val == 0 && !values.exists(doc)) return; // depend on fact that non existing values return 0 for func query

      long currVal = result[slotNum];
      if (currVal == 0 && !exists.get(slotNum)) {
        exists.set(slotNum);
        result[slotNum] = val;
      } else if (Long.compare(val, currVal) * minmax < 0) {
        result[slotNum] =  val;
      }
    }

    @Override
    public Object getValue(int slot) {
      long val = result[slot];
      if (val == 0 && !exists.get(slot)) {
        return null;
      } else {
        return val;
      }
    }

    @Override
    public void resize(Resizer resizer) {
      super.resize(resizer);
      exists = resizer.resize(exists);
    }

    @Override
    public int compare(int slotA, int slotB) {
      long a = result[slotA];
      long b = result[slotB];
      boolean ea = a != 0 || exists.get(slotA);
      boolean eb = b != 0 || exists.get(slotB);

      if (ea != eb) {
        if (ea) return 1;  // a exists and b doesn't TODO: we need context to be able to sort missing last!  SOLR-10618
        if (eb) return -1; // b exists and a is missing
      }

      return Long.compare(a, b);
    }

    @Override
    public void reset() {
      super.reset();
      exists.clear(0, exists.length());
    }

  }

  class DateFuncAcc extends SlotAcc.LongFuncSlotAcc {
    private static final long MISSING = Long.MIN_VALUE;
    public DateFuncAcc(ValueSource values, FacetContext fcontext, int numSlots) {
      super(values, fcontext, numSlots, MISSING);
    }

    @Override
    public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
      long val = values.longVal(doc);
      if (val == 0 && !values.exists(doc)) return; // depend on fact that non existing values return 0 for func query

      long currVal = result[slotNum];
      if (Long.compare(val, currVal) * minmax < 0 || currVal == MISSING) {
        result[slotNum] =  val;
      }
    }

    // let compare be the default for now (since we can't yet correctly handle sortMissingLast

    @Override
    public Object getValue(int slot) {
      return result[slot] == MISSING ? null : new Date(result[slot]);
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
    public void resetIterators() throws IOException {
      super.resetIterators();
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
      super.setNextReader(readerContext);
      if (subDvs != null) {
        subDv = subDvs[readerContext.ord];
        toGlobal = ordMap.getGlobalOrds(readerContext.ord);
        assert toGlobal != null;
      } else {
        assert readerContext.ord==0 || topLevel.getValueCount() == 0;
        subDv = topLevel;
      }
    }

    @Override
    public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
      if (subDv.advanceExact(doc)) {
        int segOrd = subDv.ordValue();
        int ord = toGlobal==null ? segOrd : (int)toGlobal.get(segOrd);
        if ((ord - slotOrd[slotNum]) * minmax < 0 || slotOrd[slotNum]==MISSING) {
          slotOrd[slotNum] = ord;
        }
      }
    }
  }

  class MinMaxSortedSetDVAcc extends DocValuesAcc {
    final static int MISSING = -1;
    SortedSetDocValues topLevel;
    SortedSetDocValues[] subDvs;
    OrdinalMap ordMap;
    LongValues toGlobal;
    SortedSetDocValues subDv;
    long[] slotOrd;

    public MinMaxSortedSetDVAcc(FacetContext fcontext, SchemaField field, int numSlots) throws IOException {
      super(fcontext, field);
      this.slotOrd = new long[numSlots];
      Arrays.fill(slotOrd, MISSING);
    }

    @Override
    public void resetIterators() throws IOException {
      super.resetIterators();
      topLevel = FieldUtil.getSortedSetDocValues(fcontext.qcontext, sf, null);
      if (topLevel instanceof MultiDocValues.MultiSortedSetDocValues) {
        ordMap = ((MultiDocValues.MultiSortedSetDocValues)topLevel).mapping;
        subDvs = ((MultiDocValues.MultiSortedSetDocValues)topLevel).values;
      } else {
        ordMap = null;
        subDvs = null;
      }
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      if (subDvs != null) {
        subDv = subDvs[readerContext.ord];
        toGlobal = ordMap.getGlobalOrds(readerContext.ord);
        assert toGlobal != null;
      } else {
        assert readerContext.ord==0 || topLevel.getValueCount() == 0;
        subDv = topLevel;
      }
    }

    @Override
    public int compare(int slotA, int slotB) {
      long a = slotOrd[slotA];
      long b = slotOrd[slotB];
      return a == MISSING ? -1: (b == MISSING? 1: Long.compare(a, b));
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      long ord = slotOrd[slotNum];
      if (ord == MISSING) return null;
      BytesRef term = topLevel.lookupOrd(ord);
      return sf.getType().toObject(sf, term);
    }

    @Override
    public void reset() throws IOException {
      Arrays.fill(slotOrd, MISSING);
    }

    @Override
    public void resize(Resizer resizer) {
      this.slotOrd = resizer.resize(slotOrd, MISSING);
    }

    @Override
    public void collectValues(int doc, int slotNum) throws IOException {
      long newOrd = MISSING;
      if (minmax == 1) {// min
        newOrd = subDv.nextOrd();
      } else  { // max
        long ord;
        while ((ord = subDv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          newOrd = ord;
        }
      }

      long currOrd = slotOrd[slotNum];
      long finalOrd = toGlobal==null ? newOrd : toGlobal.get(newOrd);
      if (currOrd == MISSING || Long.compare(finalOrd, currOrd) * minmax < 0) {
        slotOrd[slotNum] = finalOrd;
      }
    }

    @Override
    protected boolean advanceExact(int doc) throws IOException {
      return subDv.advanceExact(doc);
    }
  }
}
