package org.apache.solr.search.facet;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.agkn.hll.HLL;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;

abstract class UniqueSlotAcc extends SlotAcc {
  HLLAgg.HLLFactory factory;
  SchemaField field;
  FixedBitSet[] arr;
  int currentDocBase;
  int[] counts;  // populated with the cardinality once
  int nTerms;

  public UniqueSlotAcc(FacetContext fcontext, String field, int numSlots, HLLAgg.HLLFactory factory) throws IOException {
    super(fcontext);
    this.factory = factory;
    arr = new FixedBitSet[numSlots];
    this.field = fcontext.searcher.getSchema().getField(field);
  }

  @Override
  public void reset() {
    counts = null;
    for (FixedBitSet bits : arr) {
      if (bits == null) continue;
      bits.clear(0, bits.length());
    }
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    currentDocBase = readerContext.docBase;
  }

  @Override
  public Object getValue(int slot) throws IOException {
    if (fcontext.isShard()) {
      return getShardValue(slot);
    }
    if (counts != null) {  // will only be pre-populated if this was used for sorting.
      return counts[slot];
    }

    FixedBitSet bs = arr[slot];
    return bs==null ? 0 : bs.cardinality();
  }

  private Object getShardHLL(int slot) throws IOException {
    FixedBitSet ords = arr[slot];
    if (ords == null) return null;  // TODO: when we get to refinements, may be useful to return something???

    HLL hll = factory.getHLL();
    long maxOrd = ords.length();
    Hash.LongPair hashResult = new Hash.LongPair();
    for(int ord=-1; ++ord < maxOrd;) {
      ord = ords.nextSetBit(ord);
      if (ord == DocIdSetIterator.NO_MORE_DOCS) break;
      BytesRef val = lookupOrd(ord);
      // way to avoid recomputing hash across slots?  Prob not worth space
      Hash.murmurhash3_x64_128(val.bytes, val.offset, val.length, 0, hashResult);
      // idea: if the set is small enough, just send the hashes?  We can add at the top
      // level or even just do a hash table at the top level.
      hll.addRaw(hashResult.val1);
    }

    SimpleOrderedMap map = new SimpleOrderedMap();
    map.add("hll", hll.toBytes());
    return map;
  }

  private Object getShardValue(int slot) throws IOException {
    if (factory != null) return getShardHLL(slot);
    FixedBitSet ords = arr[slot];
    int unique;
    if (counts != null) {
      unique = counts[slot];
    } else {
      unique = ords==null ? 0 : ords.cardinality();
    }

    SimpleOrderedMap map = new SimpleOrderedMap();
    map.add("unique", unique);
    map.add("nTerms", nTerms);

    int maxExplicit=100;
    // TODO: make configurable
    // TODO: share values across buckets
    if (unique > 0) {

      List lst = new ArrayList( Math.min(unique, maxExplicit) );

      long maxOrd = ords.length();
      if (ords != null && ords.length() > 0) {
        for (int ord=0; lst.size() < maxExplicit;) {
          ord = ords.nextSetBit(ord);
          if (ord == DocIdSetIterator.NO_MORE_DOCS) break;
          BytesRef val = lookupOrd(ord);
          Object o = field.getType().toObject(field, val);
          lst.add(o);
          if (++ord >= maxOrd) break;
        }
      }

      map.add("vals", lst);
    }

    return map;
  }

  protected abstract BytesRef lookupOrd(int ord) throws IOException;

  // we only calculate all the counts when sorting by count
  public void calcCounts() {
    counts = new int[arr.length];
    for (int i=0; i<arr.length; i++) {
      FixedBitSet bs = arr[i];
      counts[i] = bs == null ? 0 : bs.cardinality();
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    if (counts == null) {  // TODO: a more efficient way to do this?  prepareSort?
      calcCounts();
    }
    return counts[slotA] - counts[slotB];
  }

  @Override
  public void resize(Resizer resizer) {
    arr = resizer.resize(arr, null);
  }
}


class UniqueSinglevaluedSlotAcc extends UniqueSlotAcc {
  final SortedDocValues topLevel;
  final SortedDocValues[] subDvs;
  final MultiDocValues.OrdinalMap ordMap;
  LongValues toGlobal;
  SortedDocValues subDv;

  public UniqueSinglevaluedSlotAcc(FacetContext fcontext, String field, int numSlots, HLLAgg.HLLFactory factory) throws IOException {
    super(fcontext, field, numSlots, factory);
    SolrIndexSearcher searcher = fcontext.qcontext.searcher();
    topLevel = FieldUtil.getSortedDocValues(fcontext.qcontext, searcher.getSchema().getField(field), null);
    nTerms = topLevel.getValueCount();
    if (topLevel instanceof MultiDocValues.MultiSortedDocValues) {
      ordMap = ((MultiDocValues.MultiSortedDocValues)topLevel).mapping;
      subDvs = ((MultiDocValues.MultiSortedDocValues)topLevel).values;
    } else {
      ordMap = null;
      subDvs = null;
    }
  }

  @Override
  protected BytesRef lookupOrd(int ord) {
    return topLevel.lookupOrd(ord);
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
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
  public void collect(int doc, int slotNum) {
    int segOrd = subDv.getOrd(doc);
    if (segOrd < 0) return;  // -1 means missing
    int ord = toGlobal==null ? segOrd : (int)toGlobal.get(segOrd);

    FixedBitSet bits = arr[slotNum];
    if (bits == null) {
      bits = new FixedBitSet(nTerms);
      arr[slotNum] = bits;
    }
    bits.set(ord);
  }
}


class UniqueMultiDvSlotAcc extends UniqueSlotAcc {
  final SortedSetDocValues topLevel;
  final SortedSetDocValues[] subDvs;
  final MultiDocValues.OrdinalMap ordMap;
  LongValues toGlobal;
  SortedSetDocValues subDv;

  public UniqueMultiDvSlotAcc(FacetContext fcontext, String field, int numSlots, HLLAgg.HLLFactory factory) throws IOException {
    super(fcontext, field, numSlots, factory);
    SolrIndexSearcher searcher = fcontext.qcontext.searcher();
    topLevel = FieldUtil.getSortedSetDocValues(fcontext.qcontext, searcher.getSchema().getField(field), null);
    nTerms = (int) topLevel.getValueCount();
    if (topLevel instanceof MultiDocValues.MultiSortedSetDocValues) {
      ordMap = ((MultiDocValues.MultiSortedSetDocValues) topLevel).mapping;
      subDvs = ((MultiDocValues.MultiSortedSetDocValues) topLevel).values;
    } else {
      ordMap = null;
      subDvs = null;
    }
  }

  @Override
  protected BytesRef lookupOrd(int ord) {
    return topLevel.lookupOrd(ord);
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
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
  public void collect(int doc, int slotNum) {
    subDv.setDocument(doc);
    int segOrd = (int) subDv.nextOrd();
    if (segOrd < 0) return;

    FixedBitSet bits = arr[slotNum];
    if (bits == null) {
      bits = new FixedBitSet(nTerms);
      arr[slotNum] = bits;
    }

    do {
      int ord = toGlobal == null ? segOrd : (int) toGlobal.get(segOrd);
      bits.set(ord);
      segOrd = (int) subDv.nextOrd();
    } while (segOrd >= 0);
  }
}



class UniqueMultivaluedSlotAcc extends UniqueSlotAcc implements UnInvertedField.Callback {
  private UnInvertedField uif;
  private UnInvertedField.DocToTerm docToTerm;

  public UniqueMultivaluedSlotAcc(FacetContext fcontext, String field, int numSlots, HLLAgg.HLLFactory factory) throws IOException {
    super(fcontext, field, numSlots, factory);
    SolrIndexSearcher searcher = fcontext.qcontext.searcher();
    uif = UnInvertedField.getUnInvertedField(field, searcher);
    docToTerm = uif.new DocToTerm();
    fcontext.qcontext.addCloseHook(this);  // TODO: find way to close accumulators instead of using close hook?
    nTerms = uif.numTerms();
  }

  @Override
  protected BytesRef lookupOrd(int ord) throws IOException {
    return docToTerm.lookupOrd(ord);
  }

  private FixedBitSet bits;  // bits for the current slot, only set for the callback

  @Override
  public void call(int termNum) {
    bits.set(termNum);
  }

  @Override
  public void collect(int doc, int slotNum) throws IOException {
    bits = arr[slotNum];
    if (bits == null) {
      bits = new FixedBitSet(nTerms);
      arr[slotNum] = bits;
    }
    docToTerm.getTerms(doc + currentDocBase, this);  // this will call back to our Callback.call(int termNum)
  }

  @Override
  public void close() throws IOException {
    if (docToTerm != null) {
      docToTerm.close();
      docToTerm = null;
    }
  }
}