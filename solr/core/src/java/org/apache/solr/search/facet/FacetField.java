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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiPostingsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.Filter;
import org.apache.solr.search.HashDocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSet;


public class FacetField extends FacetRequest {
  String field;
  long offset;
  long limit = 10;
  long mincount = 1;
  boolean missing;
  boolean allBuckets;   // show cumulative stats across all buckets (this can be different than non-bucketed stats across all docs because of multi-valued docs)
  boolean numBuckets;
  String prefix;
  String sortVariable;
  SortDirection sortDirection;
  FacetMethod method;
  int cacheDf;  // 0 means "default", -1 means "never cache"

  // TODO: put this somewhere more generic?
  public static enum SortDirection {
    asc(-1) ,
    desc(1);

    private final int multiplier;
    private SortDirection(int multiplier) {
      this.multiplier = multiplier;
    }

    // asc==-1, desc==1
    public int getMultiplier() {
      return multiplier;
    }
  }

  public static enum FacetMethod {
    ENUM,
    STREAM,
    FIELDCACHE,
    SMART,
    ;

    public static FacetMethod fromString(String method) {
      if (method == null || method.length()==0) return null;
      if ("enum".equals(method)) {
        return ENUM;
      } else if ("fc".equals(method) || "fieldcache".equals(method)) {
        return FIELDCACHE;
      } else if ("smart".equals(method)) {
        return SMART;
      } else if ("stream".equals(method)) {
        return STREAM;
      }
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown FacetField method " + method);
    }
  }


  @Override
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    SchemaField sf = fcontext.searcher.getSchema().getField(field);
    FieldType ft = sf.getType();
    boolean multiToken = sf.multiValued() || ft.multiValuedFieldCache();

    if (method == FacetMethod.ENUM && sf.indexed()) {
      throw new UnsupportedOperationException();
    } else if (method == FacetMethod.STREAM && sf.indexed()) {
      return new FacetFieldProcessorStream(fcontext, this, sf);
    }

    org.apache.lucene.document.FieldType.NumericType ntype = ft.getNumericType();

    if (sf.hasDocValues() && ntype==null) {
      // single and multi-valued string docValues
      return new FacetFieldProcessorDV(fcontext, this, sf);
    }

    if (!multiToken) {
      if (sf.getType().getNumericType() != null) {
        // single valued numeric (docvalues or fieldcache)
        return new FacetFieldProcessorNumeric(fcontext, this, sf);
      } else {
        // single valued string...
//        return new FacetFieldProcessorDV(fcontext, this, sf);
        return new FacetFieldProcessorDV(fcontext, this, sf);
        // what about FacetFieldProcessorFC?
      }
    }

    // Multi-valued field cache (UIF)
    return new FacetFieldProcessorUIF(fcontext, this, sf);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetFieldMerger(this);
  }
}



abstract class FacetFieldProcessor extends FacetProcessor<FacetField> {
  SchemaField sf;
  SlotAcc indexOrderAcc;
  int effectiveMincount;

  Map<String,AggValueSource> deferredAggs;  // null if none

  // TODO: push any of this down to base class?

  //
  // For sort="x desc", collectAcc would point to "x", and sortAcc would also point to "x".
  // collectAcc would be used to accumulate all buckets, and sortAcc would be used to sort those buckets.
  //
  SlotAcc collectAcc;  // Accumulator to collect across entire domain (in addition to the countAcc).  May be null.
  SlotAcc sortAcc;     // Accumulator to use for sorting *only* (i.e. not used for collection). May be an alias of countAcc, collectAcc, or indexOrderAcc
  SlotAcc[] otherAccs; // Accumulators that do not need to be calculated across all buckets.

  SpecialSlotAcc allBucketsAcc;  // this can internally refer to otherAccs and/or collectAcc. setNextReader should be called on otherAccs directly if they exist.


  FacetFieldProcessor(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq);
    this.sf = sf;
    this.effectiveMincount = (int)(fcontext.isShard() ? Math.min(1 , freq.mincount) : freq.mincount);
  }

  @Override
  public Object getResponse() {
    return response;
  }

  // This is used to create accs for second phase (or to create accs for all aggs)
  @Override
  protected void createAccs(int docCount, int slotCount) throws IOException {
    if (accMap == null) {
      accMap = new LinkedHashMap<>();
    }

    // allow a custom count acc to be used
    if (countAcc == null) {
      countAcc = new CountSlotArrAcc(fcontext, slotCount);
      countAcc.key = "count";
    }

    if (accs != null) {
      // reuse these accs, but reset them first
      for (SlotAcc acc : accs) {
        acc.reset();
      }
      return;
    } else {
      accs = new SlotAcc[ freq.getFacetStats().size() ];
    }

    int accIdx = 0;
    for (Map.Entry<String,AggValueSource> entry : freq.getFacetStats().entrySet()) {
      SlotAcc acc = null;
      if (slotCount == 1) {
        acc = accMap.get(entry.getKey());
        if (acc != null) {
          acc.reset();
        }
      }
      if (acc == null) {
        acc = entry.getValue().createSlotAcc(fcontext, docCount, slotCount);
        acc.key = entry.getKey();
        accMap.put(acc.key, acc);
      }
      accs[accIdx++] = acc;
    }
  }

  void createCollectAcc(int numDocs, int numSlots) throws IOException {
    accMap = new LinkedHashMap<>();

    // we always count...
    // allow a subclass to set a custom counter.
    if (countAcc == null) {
      countAcc = new CountSlotArrAcc(fcontext, numSlots);
    }

    if ("count".equals(freq.sortVariable)) {
      sortAcc = countAcc;
      deferredAggs = freq.getFacetStats();
    } else if ("index".equals(freq.sortVariable)) {
      // allow subclass to set indexOrderAcc first
      if (indexOrderAcc == null) {
        // This sorting accumulator just goes by the slot number, so does not need to be collected
        // and hence does not need to find it's way into the accMap or accs array.
        indexOrderAcc = new SortSlotAcc(fcontext);
      }
      sortAcc = indexOrderAcc;
      deferredAggs = freq.getFacetStats();
    } else {
      AggValueSource sortAgg = freq.getFacetStats().get(freq.sortVariable);
      if (sortAgg != null) {
        collectAcc = sortAgg.createSlotAcc(fcontext, numDocs, numSlots);
        collectAcc.key = freq.sortVariable; // TODO: improve this
      }
      sortAcc = collectAcc;
      deferredAggs = new HashMap<>(freq.getFacetStats());
      deferredAggs.remove(freq.sortVariable);
    }

    if (deferredAggs.size() == 0) {
      deferredAggs = null;
    }

    boolean needOtherAccs = freq.allBuckets;  // TODO: use for missing too...

    if (!needOtherAccs) {
      // we may need them later, but we don't want to create them now
      // otherwise we won't know if we need to call setNextReader on them.
      return;
    }

    // create the deffered aggs up front for use by allBuckets
    createOtherAccs(numDocs, 1);
  }


  void createOtherAccs(int numDocs, int numSlots) throws IOException {
    if (otherAccs != null) {
      // reuse existing accumulators
      for (SlotAcc acc : otherAccs) {
        acc.reset();  // todo - make reset take numDocs and numSlots?
      }
      return;
    }

    int numDeferred = deferredAggs == null ? 0 : deferredAggs.size();
    if (numDeferred <= 0) return;

    otherAccs = new SlotAcc[ numDeferred ];

    int otherAccIdx = 0;
    for (Map.Entry<String,AggValueSource> entry : deferredAggs.entrySet()) {
      AggValueSource agg = entry.getValue();
      SlotAcc acc = agg.createSlotAcc(fcontext, numDocs, numSlots);
      acc.key = entry.getKey();
      accMap.put(acc.key, acc);
      otherAccs[otherAccIdx++] = acc;
    }

    if (numDeferred == freq.getFacetStats().size()) {
      // accs and otherAccs are the same...
      accs = otherAccs;
    }
  }


  int collectFirstPhase(DocSet docs, int slot) throws IOException {
    int num = -1;
    if (collectAcc != null) {
      num = collectAcc.collect(docs, slot);
    }
    if (allBucketsAcc != null) {
      num = allBucketsAcc.collect(docs, slot);
    }
    return num >= 0 ? num : docs.size();
  }

  void collectFirstPhase(int segDoc, int slot) throws IOException {
    if (collectAcc != null) {
      collectAcc.collect(segDoc, slot);
    }
    if (allBucketsAcc != null) {
      allBucketsAcc.collect(segDoc, slot);
    }
  }


  void fillBucket(SimpleOrderedMap<Object> target, int count, int slotNum, DocSet subDomain, Query filter) throws IOException {
    target.add("count", count);
    if (count <= 0 && !freq.processEmpty) return;

    if (collectAcc != null && slotNum >= 0) {
      collectAcc.setValues(target, slotNum);
    }

    createOtherAccs(-1, 1);

    if (otherAccs == null && freq.subFacets.isEmpty()) return;

    if (subDomain == null) {
      subDomain = fcontext.searcher.getDocSet(filter, fcontext.base);
    }

    // if no subFacets, we only need a DocSet
    // otherwise we need more?
    // TODO: save something generic like "slotNum" in the context and use that to implement things like filter exclusion if necessary?
    // Hmmm, but we need to look up some stuff anyway (for the label?)
    // have a method like "DocSet applyConstraint(facet context, DocSet parent)"
    // that's needed for domain changing things like joins anyway???

    if (otherAccs != null) {
      // do acc at a time (traversing domain each time) or do all accs for each doc?
      for (SlotAcc acc : otherAccs) {
        acc.reset(); // TODO: only needed if we previously used for allBuckets or missing
        acc.collect(subDomain, 0);
        acc.setValues(target, 0);
      }
    }

    processSubs(target, filter, subDomain);
  }


  @Override
  protected void processStats(SimpleOrderedMap<Object> bucket, DocSet docs, int docCount) throws IOException {
    if (docCount == 0 && !freq.processEmpty || freq.getFacetStats().size() == 0) {
      bucket.add("count", docCount);
      return;
    }
    createAccs(docCount, 1);
    int collected = collect(docs, 0);

    // countAcc.incrementCount(0, collected);  // should we set the counton the acc instead of just passing it?

    assert collected == docCount;
    addStats(bucket, collected, 0);
  }

  // overrides but with different signature!
  void addStats(SimpleOrderedMap<Object> target, int count, int slotNum) throws IOException {
    target.add("count", count);
    if (count > 0 || freq.processEmpty) {
      for (SlotAcc acc : accs) {
        acc.setValues(target, slotNum);
      }
    }
  }

  @Override
  void setNextReader(LeafReaderContext ctx) throws IOException {
    // base class calls this (for missing bucket...) ...  go over accs[] in that case
    super.setNextReader(ctx);
  }

  void setNextReaderFirstPhase(LeafReaderContext ctx) throws IOException {
    if (collectAcc != null) {
      collectAcc.setNextReader(ctx);
    }
    if (otherAccs != null) {
      for (SlotAcc acc : otherAccs) {
        acc.setNextReader(ctx);
      }
    }
  }

  static class Slot {
    int slot;
    public int tiebreakCompare(int slotA, int slotB) {
      return slotB - slotA;
    }
  }
}

class SpecialSlotAcc extends SlotAcc {
  SlotAcc collectAcc;
  SlotAcc[] otherAccs;
  int collectAccSlot;
  int otherAccsSlot;
  long count;

  public SpecialSlotAcc(FacetContext fcontext, SlotAcc collectAcc, int collectAccSlot, SlotAcc[] otherAccs, int otherAccsSlot) {
    super(fcontext);
    this.collectAcc = collectAcc;
    this.collectAccSlot = collectAccSlot;
    this.otherAccs = otherAccs;
    this.otherAccsSlot = otherAccsSlot;
  }

  public int getCollectAccSlot() { return collectAccSlot; }
  public int getOtherAccSlot() { return otherAccsSlot; }

  public long getSpecialCount() {
    return count;
  }

  @Override
  public void collect(int doc, int slot) throws IOException {
    assert slot != collectAccSlot || slot < 0;
    count++;
    if (collectAcc != null) {
      collectAcc.collect(doc, collectAccSlot);
    }
    if (otherAccs != null) {
      for (SlotAcc otherAcc : otherAccs) {
        otherAcc.collect(doc, otherAccsSlot);
      }
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getValue(int slotNum) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
    if (collectAcc != null) {
      collectAcc.setValues(bucket, collectAccSlot);
    }
    if (otherAccs != null) {
      for (SlotAcc otherAcc : otherAccs) {
        otherAcc.setValues(bucket, otherAccsSlot);
      }
    }
  }

  @Override
  public void reset() {
    // reset should be called on underlying accs
    // TODO: but in case something does need to be done here, should we require this method to be called but do nothing for now?
    throw new UnsupportedOperationException();
  }

  @Override
  public void resize(Resizer resizer) {
    // someone else will call resize on collectAcc directly
    if (collectAccSlot >= 0) {
      collectAccSlot = resizer.getNewSlot(collectAccSlot);
    }
  }
}




// base class for FC style of facet counting (single and multi-valued strings)
abstract class FacetFieldProcessorFCBase extends FacetFieldProcessor {
  BytesRefBuilder prefixRef;
  int startTermIndex;
  int endTermIndex;
  int nTerms;
  int nDocs;
  int maxSlots;

  int allBucketsSlot = -1;  // slot for the primary Accs (countAcc, collectAcc)

  public FacetFieldProcessorFCBase(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  @Override
  public void process() throws IOException {
    super.process();
    sf = fcontext.searcher.getSchema().getField(freq.field);
    response = getFieldCacheCounts();
  }


  /** this BytesRef may be shared across calls and should be deep-cloned if necessary */
  abstract protected BytesRef lookupOrd(int ord) throws IOException;
  abstract protected void findStartAndEndOrds() throws IOException;
  abstract protected void collectDocs() throws IOException;


  public SimpleOrderedMap<Object> getFieldCacheCounts() throws IOException {
    String prefix = freq.prefix;
    if (prefix == null || prefix.length() == 0) {
      prefixRef = null;
    } else {
      prefixRef = new BytesRefBuilder();
      prefixRef.copyChars(prefix);
    }

    findStartAndEndOrds();

    maxSlots = nTerms;

    if (freq.allBuckets) {
      allBucketsSlot = maxSlots++;
    }

    createCollectAcc(nDocs, maxSlots);

    if (freq.allBuckets) {
      allBucketsAcc = new SpecialSlotAcc(fcontext, collectAcc, allBucketsSlot, otherAccs, 0);
    }

    collectDocs();

    return findTopSlots();
  }


  protected SimpleOrderedMap<Object> findTopSlots() throws IOException {
    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();

    int numBuckets = 0;
    List<Object> bucketVals = null;
    if (freq.numBuckets && fcontext.isShard()) {
      bucketVals = new ArrayList(100);
    }

    int off = fcontext.isShard() ? 0 : (int) freq.offset;
    // add a modest amount of over-request if this is a shard request
    int lim = freq.limit >= 0 ? (fcontext.isShard() ? (int)(freq.limit*1.1+4) : (int)freq.limit) : Integer.MAX_VALUE;

    int maxsize = (int)(freq.limit > 0 ?  freq.offset + lim : Integer.MAX_VALUE - 1);
    maxsize = Math.min(maxsize, nTerms);

    final int sortMul = freq.sortDirection.getMultiplier();
    final SlotAcc sortAcc = this.sortAcc;

    PriorityQueue<Slot> queue = new PriorityQueue<Slot>(maxsize) {
      @Override
      protected boolean lessThan(Slot a, Slot b) {
        int cmp = sortAcc.compare(a.slot, b.slot) * sortMul;
        return cmp == 0 ? b.slot < a.slot : cmp < 0;
      }
    };

    Slot bottom = null;
    for (int i = 0; i < nTerms; i++) {
      // screen out buckets not matching mincount immediately (i.e. don't even increment numBuckets)
      if (effectiveMincount > 0 && countAcc.getCount(i) < effectiveMincount) {
        continue;
      }

      numBuckets++;
      if (bucketVals != null && bucketVals.size()<100) {
        int ord = startTermIndex + i;
        BytesRef br = lookupOrd(ord);
        Object val = sf.getType().toObject(sf, br);
        bucketVals.add(val);
      }


      if (bottom != null) {
        if (sortAcc.compare(bottom.slot, i) * sortMul < 0) {
          bottom.slot = i;
          bottom = queue.updateTop();
        }
      } else {
        // queue not full
        Slot s = new Slot();
        s.slot = i;
        queue.add(s);
        if (queue.size() >= maxsize) {
          bottom = queue.top();
        }
      }
    }

    if (freq.numBuckets) {
      if (!fcontext.isShard()) {
        res.add("numBuckets", numBuckets);
      } else {
        SimpleOrderedMap map = new SimpleOrderedMap(2);
        map.add("numBuckets", numBuckets);
        map.add("vals", bucketVals);
        res.add("numBuckets", map);
      }
    }

    // if we are deep paging, we don't have to order the highest "offset" counts.
    int collectCount = Math.max(0, queue.size() - off);
    assert collectCount <= lim;
    int[] sortedSlots = new int[collectCount];
    for (int i = collectCount - 1; i >= 0; i--) {
      sortedSlots[i] = queue.pop().slot;
    }

    if (freq.allBuckets) {
      SimpleOrderedMap<Object> allBuckets = new SimpleOrderedMap<>();
      allBuckets.add("count", allBucketsAcc.getSpecialCount());
      if (allBucketsAcc != null) {
        allBucketsAcc.setValues(allBuckets, allBucketsSlot);
      }
      res.add("allBuckets", allBuckets);
    }

    ArrayList bucketList = new ArrayList(collectCount);
    res.add("buckets", bucketList);


    // TODO: do this with a callback instead?
    boolean needFilter = deferredAggs != null || freq.getSubFacets().size() > 0;

    for (int slotNum : sortedSlots) {
      SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();

      // get the ord of the slot...
      int ord = startTermIndex + slotNum;

      BytesRef br = lookupOrd(ord);
      Object val = sf.getType().toObject(sf, br);

      bucket.add("val", val);

      TermQuery filter = needFilter ? new TermQuery(new Term(sf.getName(), BytesRef.deepCopyOf(br))) : null;
      fillBucket(bucket, countAcc.getCount(slotNum), slotNum, null, filter);

      bucketList.add(bucket);
    }

    if (freq.missing) {
      SimpleOrderedMap<Object> missingBucket = new SimpleOrderedMap<>();
      fillBucket(missingBucket, getFieldMissingQuery(fcontext.searcher, freq.field), null);
      res.add("missing", missingBucket);
    }

    return res;
  }


}


class FacetFieldProcessorDV extends FacetFieldProcessorFCBase {
  static boolean unwrap_singleValued_multiDv = true;  // only set to false for test coverage

  boolean multiValuedField;
  SortedSetDocValues si;  // only used for term lookups (for both single and multi-valued)
  MultiDocValues.OrdinalMap ordinalMap = null; // maps per-segment ords to global ords


  public FacetFieldProcessorDV(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
    multiValuedField = sf.multiValued() || sf.getType().multiValuedFieldCache();
  }

  protected BytesRef lookupOrd(int ord) throws IOException {
    return si.lookupOrd(ord);
  }

  protected void findStartAndEndOrds() throws IOException {
    if (multiValuedField) {
      si = FieldUtil.getSortedSetDocValues(fcontext.qcontext, sf, null);
      if (si instanceof MultiDocValues.MultiSortedSetDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedSetDocValues)si).mapping;
      }
    } else {
      SortedDocValues single = FieldUtil.getSortedDocValues(fcontext.qcontext, sf, null);
      si = DocValues.singleton(single);  // multi-valued view
      if (single instanceof MultiDocValues.MultiSortedDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedDocValues)single).mapping;
      }
    }

    if (si.getValueCount() >= Integer.MAX_VALUE) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Field has too many unique values. field=" + sf + " nterms= " + si.getValueCount());
    }

    if (prefixRef != null) {
      startTermIndex = (int)si.lookupTerm(prefixRef.get());
      if (startTermIndex < 0) startTermIndex = -startTermIndex - 1;
      prefixRef.append(UnicodeUtil.BIG_TERM);
      endTermIndex = (int)si.lookupTerm(prefixRef.get());
      assert endTermIndex < 0;
      endTermIndex = -endTermIndex - 1;
    } else {
      startTermIndex = 0;
      endTermIndex = (int)si.getValueCount();
    }

    nTerms = endTermIndex - startTermIndex;
  }

  @Override
  protected void collectDocs() throws IOException {
    if (nTerms <= 0 || fcontext.base.size() < effectiveMincount) { // TODO: what about allBuckets? missing bucket?
      return;
    }

    final List<LeafReaderContext> leaves = fcontext.searcher.getIndexReader().leaves();
    Filter filter = fcontext.base.getTopFilter();

    for (int subIdx = 0; subIdx < leaves.size(); subIdx++) {
      LeafReaderContext subCtx = leaves.get(subIdx);

      setNextReaderFirstPhase(subCtx);

      DocIdSet dis = filter.getDocIdSet(subCtx, null); // solr docsets already exclude any deleted docs
      DocIdSetIterator disi = dis.iterator();

      SortedDocValues singleDv = null;
      SortedSetDocValues multiDv = null;
      if (multiValuedField) {
        // TODO: get sub from multi?
        multiDv = subCtx.reader().getSortedSetDocValues(sf.getName());
        if (multiDv == null) {
          multiDv = DocValues.emptySortedSet();
        }
        // some codecs may optimize SortedSet storage for single-valued fields
        // this will be null if this is not a wrapped single valued docvalues.
        if (unwrap_singleValued_multiDv) {
          singleDv = DocValues.unwrapSingleton(multiDv);
        }
      } else {
        singleDv = subCtx.reader().getSortedDocValues(sf.getName());
        if (singleDv == null) {
          singleDv = DocValues.emptySorted();
        }
      }

      LongValues toGlobal = ordinalMap == null ? null : ordinalMap.getGlobalOrds(subIdx);

      if (singleDv != null) {
        collectDocs(singleDv, disi, toGlobal);
      } else {
        collectDocs(multiDv, disi, toGlobal);
      }
    }

  }

  protected void collectDocs(SortedDocValues singleDv, DocIdSetIterator disi, LongValues toGlobal) throws IOException {
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      int segOrd = singleDv.getOrd(doc);
      if (segOrd < 0) continue;
      collect(doc, segOrd, toGlobal);
    }
  }

  protected void collectDocs(SortedSetDocValues multiDv, DocIdSetIterator disi, LongValues toGlobal) throws IOException {
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      multiDv.setDocument(doc);
      for(;;) {
        int segOrd = (int)multiDv.nextOrd();
        if (segOrd < 0) break;
        collect(doc, segOrd, toGlobal);
      }
    }
  }

  private void collect(int doc, int segOrd, LongValues toGlobal) throws IOException {
    int ord = (toGlobal != null && segOrd >= 0) ? (int)toGlobal.get(segOrd) : segOrd;

    int arrIdx = ord - startTermIndex;
    if (arrIdx >= 0 && arrIdx < nTerms) {
      countAcc.incrementCount(arrIdx, 1);
      if (collectAcc != null) {
        collectAcc.collect(doc, arrIdx);
      }
      if (allBucketsAcc != null) {
        allBucketsAcc.collect(doc, arrIdx);
      }
    }
  }

}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

















// UnInvertedField implementation of field faceting
class FacetFieldProcessorUIF extends FacetFieldProcessorFCBase {
  UnInvertedField uif;
  TermsEnum te;

  FacetFieldProcessorUIF(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  @Override
  protected void findStartAndEndOrds() throws IOException {
    uif = UnInvertedField.getUnInvertedField(freq.field, fcontext.searcher);
    te = uif.getOrdTermsEnum( fcontext.searcher.getLeafReader() );    // "te" can be null

    startTermIndex = 0;
    endTermIndex = uif.numTerms();  // one past the end

    if (prefixRef != null && te != null) {
      if (te.seekCeil(prefixRef.get()) == TermsEnum.SeekStatus.END) {
        startTermIndex = uif.numTerms();
      } else {
        startTermIndex = (int) te.ord();
      }
      prefixRef.append(UnicodeUtil.BIG_TERM);
      if (te.seekCeil(prefixRef.get()) == TermsEnum.SeekStatus.END) {
        endTermIndex = uif.numTerms();
      } else {
        endTermIndex = (int) te.ord();
      }
    }

    nTerms = endTermIndex - startTermIndex;
  }

  @Override
  protected BytesRef lookupOrd(int ord) throws IOException {
    return uif.getTermValue(te, ord);
  }

  @Override
  protected void collectDocs() throws IOException {
    uif.collectDocs(this);
  }
}



class FacetFieldProcessorStream extends FacetFieldProcessor implements Closeable {
  long bucketsToSkip;
  long bucketsReturned;

  boolean closed;
  boolean countOnly;
  boolean hasSubFacets;  // true if there are subfacets
  int minDfFilterCache;
  DocSet docs;
  DocSet fastForRandomSet;
  TermsEnum termsEnum = null;
  SolrIndexSearcher.DocsEnumState deState = null;
  PostingsEnum postingsEnum;
  BytesRef startTermBytes;
  BytesRef term;
  LeafReaderContext[] leaves;



  FacetFieldProcessorStream(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      // fcontext.base.decref();  // OFF-HEAP
    }
  }


  @Override
  public void process() throws IOException {
    super.process();

    // We need to keep the fcontext open after processing is done (since we will be streaming in the response writer).
    // But if the connection is broken, we want to clean up.
    // fcontext.base.incref();  // OFF-HEAP
    fcontext.qcontext.addCloseHook(this);

    setup();
    response = new SimpleOrderedMap<>();
    response.add( "buckets", new Iterator() {
      boolean retrieveNext = true;
      Object val;
      @Override
      public boolean hasNext() {
        if (retrieveNext) {
          val = nextBucket();
        }
        retrieveNext = false;
        return val != null;
      }

      @Override
      public Object next() {
        if (retrieveNext) {
          val = nextBucket();
        }
        retrieveNext = true;
        if (val == null) {
          // Last value, so clean up.  In the case that we are doing streaming facets within streaming facets,
          // the number of close hooks could grow very large, so we want to remove ourselves.
          boolean removed = fcontext.qcontext.removeCloseHook(FacetFieldProcessorStream.this);
          assert removed;
          try {
            close();
          } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error during facet streaming close", e);
          }
        }
        return val;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    });
  }



  public void setup() throws IOException {

    countOnly = freq.facetStats.size() == 0 || freq.facetStats.values().iterator().next() instanceof CountAgg;
    hasSubFacets = freq.subFacets.size() > 0;
    bucketsToSkip = freq.offset;

    createAccs(-1, 1);

    // Minimum term docFreq in order to use the filterCache for that term.
    int defaultMinDf = Math.max(fcontext.searcher.maxDoc() >> 4, 3);  // (minimum of 3 is for test coverage purposes)
    int minDfFilterCache = freq.cacheDf == 0 ? defaultMinDf : freq.cacheDf;
    if (minDfFilterCache == -1) minDfFilterCache = Integer.MAX_VALUE;  // -1 means never cache

    docs = fcontext.base;
    fastForRandomSet = null;

    if (freq.prefix != null) {
      String indexedPrefix = sf.getType().toInternal(freq.prefix);
      startTermBytes = new BytesRef(indexedPrefix);
    } else if (sf.getType().getNumericType() != null) {
      String triePrefix = TrieField.getMainValuePrefix(sf.getType());
      if (triePrefix != null) {
        startTermBytes = new BytesRef(triePrefix);
      }
    }

    Fields fields = fcontext.searcher.getLeafReader().fields();
    Terms terms = fields == null ? null : fields.terms(sf.getName());


    termsEnum = null;
    deState = null;
    term = null;


    if (terms != null) {

      termsEnum = terms.iterator();

      // TODO: OPT: if seek(ord) is supported for this termsEnum, then we could use it for
      // facet.offset when sorting by index order.

      if (startTermBytes != null) {
        if (termsEnum.seekCeil(startTermBytes) == TermsEnum.SeekStatus.END) {
          termsEnum = null;
        } else {
          term = termsEnum.term();
        }
      } else {
        // position termsEnum on first term
        term = termsEnum.next();
      }
    }

    List<LeafReaderContext> leafList = fcontext.searcher.getTopReaderContext().leaves();
    leaves = leafList.toArray( new LeafReaderContext[ leafList.size() ]);
  }


  public SimpleOrderedMap<Object> nextBucket() {
    try {
      return _nextBucket();
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error during facet streaming", e);
    }
  }

  public SimpleOrderedMap<Object> _nextBucket() throws IOException {
    DocSet termSet = null;

    try {
      while (term != null) {

        if (startTermBytes != null && !StringHelper.startsWith(term, startTermBytes)) {
          break;
        }

        int df = termsEnum.docFreq();
        if (df < effectiveMincount) {
          term = termsEnum.next();
          continue;
        }

        if (termSet != null) {
          // termSet.decref(); // OFF-HEAP
          termSet = null;
        }

        int c = 0;

        if (hasSubFacets || df >= minDfFilterCache) {
          // use the filter cache

          if (deState == null) {
            deState = new SolrIndexSearcher.DocsEnumState();
            deState.fieldName = sf.getName();
            deState.liveDocs = fcontext.searcher.getLeafReader().getLiveDocs();
            deState.termsEnum = termsEnum;
            deState.postingsEnum = postingsEnum;
            deState.minSetSizeCached = minDfFilterCache;
          }

            if (hasSubFacets || !countOnly) {
              DocSet termsAll = fcontext.searcher.getDocSet(deState);
              termSet = docs.intersection(termsAll);
              // termsAll.decref(); // OFF-HEAP
              c = termSet.size();
            } else {
              c = fcontext.searcher.numDocs(docs, deState);
            }
            postingsEnum = deState.postingsEnum;

            resetStats();

            if (!countOnly) {
              collect(termSet, 0);
            }

        } else {
          // We don't need the docset here (meaning no sub-facets).
          // if countOnly, then we are calculating some other stats...
          resetStats();

          // lazy convert to fastForRandomSet
          if (fastForRandomSet == null) {
            fastForRandomSet = docs;
            if (docs instanceof SortedIntDocSet) {  // OFF-HEAP todo: also check for native version
              SortedIntDocSet sset = (SortedIntDocSet) docs;
              fastForRandomSet = new HashDocSet(sset.getDocs(), 0, sset.size());
            }
          }
          // iterate over TermDocs to calculate the intersection
          postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);

          if (postingsEnum instanceof MultiPostingsEnum) {
            MultiPostingsEnum.EnumWithSlice[] subs = ((MultiPostingsEnum) postingsEnum).getSubs();
            int numSubs = ((MultiPostingsEnum) postingsEnum).getNumSubs();
            for (int subindex = 0; subindex < numSubs; subindex++) {
              MultiPostingsEnum.EnumWithSlice sub = subs[subindex];
              if (sub.postingsEnum == null) continue;
              int base = sub.slice.start;
              int docid;

              if (countOnly) {
                while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (fastForRandomSet.exists(docid + base)) c++;
                }
              } else {
                setNextReader(leaves[sub.slice.readerIndex]);
                while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (fastForRandomSet.exists(docid + base)) {
                    c++;
                    collect(docid, 0);
                  }
                }
              }

            }
          } else {
            int docid;
            if (countOnly) {
              while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid)) c++;
              }
            } else {
              setNextReader(leaves[0]);
              while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid)) {
                  c++;
                  collect(docid, 0);
                }
              }
            }
          }

        }



        if (c < effectiveMincount) {
          term = termsEnum.next();
          continue;
        }

        // handle offset and limit
        if (bucketsToSkip > 0) {
          bucketsToSkip--;
          term = termsEnum.next();
          continue;
        }

        if (freq.limit >= 0 && ++bucketsReturned > freq.limit) {
          return null;
        }

        // set count in case other stats depend on it
        countAcc.incrementCount(0, c);

        // OK, we have a good bucket to return... first get bucket value before moving to next term
        Object bucketVal = sf.getType().toObject(sf, term);
        BytesRef termCopy = BytesRef.deepCopyOf(term);
        term = termsEnum.next();

        SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
        bucket.add("val", bucketVal);
        addStats(bucket, 0);
        if (hasSubFacets) {
          TermQuery filter = new TermQuery(new Term(freq.field, termCopy));
          processSubs(bucket, filter, termSet);
        }

        // TODO... termSet needs to stick around for streaming sub-facets?

        return bucket;

      }

    } finally {
      if (termSet != null) {
        // termSet.decref();  // OFF-HEAP
        termSet = null;
      }
    }


    // end of the iteration
    return null;
  }



}


