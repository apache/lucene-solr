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
import java.util.Iterator;
import java.util.List;

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
import org.apache.lucene.search.Filter;
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
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.HashDocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSet;


public class FacetField extends FacetRequest {
  String field;
  long offset;
  long limit = 10;
  long mincount = 1;
  boolean missing;
  boolean numBuckets;
  String prefix;
  String sortVariable;
  SortDirection sortDirection;
  FacetMethod method;
  boolean allBuckets;   // show cumulative stats across all buckets (this can be different than non-bucketed stats across all docs because of multi-valued docs)
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

    if (!multiToken || sf.hasDocValues()) {
      return new FacetFieldProcessorDV(fcontext, this, sf);
    }

    if (multiToken) {
      return new FacetFieldProcessorUIF(fcontext, this, sf);
    } else {
      // single valued string
      return new FacetFieldProcessorFC(fcontext, this, sf);
    }
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetFieldMerger(this);
  }
}



abstract class FacetFieldProcessor extends FacetProcessor<FacetField> {
  SchemaField sf;
  SlotAcc sortAcc;
  int effectiveMincount;

  FacetFieldProcessor(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq);
    this.sf = sf;
    this.effectiveMincount = (int)(fcontext.isShard() ? Math.min(1 , freq.mincount) : freq.mincount);
  }

  @Override
  public Object getResponse() {
    return response;
  }

  void setSortAcc(int numSlots) {
    String sortKey = freq.sortVariable;
    sortAcc = accMap.get(sortKey);

    if (sortAcc == null) {
      if ("count".equals(sortKey)) {
        sortAcc = countAcc;
      } else if ("index".equals(sortKey)) {
        sortAcc = new SortSlotAcc(fcontext);
        // This sorting accumulator just goes by the slot number, so does not need to be collected
        // and hence does not need to find it's way into the accMap or accs array.
      }
    }
  }

  static class Slot {
    int slot;
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
  int allBucketsSlot;


  public FacetFieldProcessorFCBase(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  @Override
  public void process() throws IOException {
    sf = fcontext.searcher.getSchema().getField(freq.field);
    response = getFieldCacheCounts();
  }


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

    // if we need an extra slot for the "missing" bucket, and it wasn't able to be tacked onto the beginning,
    // then lets add room for it at the end.
    maxSlots = (freq.missing && startTermIndex != -1) ? nTerms + 1 : nTerms;

    if (freq.allBuckets) {
      allBucketsSlot = maxSlots;
      maxSlots++;
    } else {
      allBucketsSlot = -1;
    }
    createAccs(nDocs, maxSlots);
    setSortAcc(maxSlots);
    prepareForCollection();

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

    int maxsize = freq.limit > 0 ?  off + lim : Integer.MAX_VALUE - 1;
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
    for (int i = (startTermIndex == -1) ? 1 : 0; i < nTerms; i++) {
      if (countAcc.getCount(i) < effectiveMincount) {
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
      countAcc.setValues(allBuckets, allBucketsSlot);
      for (SlotAcc acc : accs) {
        acc.setValues(allBuckets, allBucketsSlot);
      }
      res.add("allBuckets", allBuckets);
    }

    ArrayList bucketList = new ArrayList(collectCount);
    res.add("buckets", bucketList);


    for (int slotNum : sortedSlots) {
      SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();

      // get the ord of the slot...
      int ord = startTermIndex + slotNum;

      BytesRef br = lookupOrd(ord);
      Object val = sf.getType().toObject(sf, br);

      bucket.add("val", val);
      // add stats for this bucket
      addStats(bucket, slotNum);

      // handle sub-facets for this bucket
      if (freq.getSubFacets().size() > 0) {
        FacetContext subContext = fcontext.sub();
        subContext.base = fcontext.searcher.getDocSet(new TermQuery(new Term(sf.getName(), br.clone())), fcontext.base);
        try {
          fillBucketSubs(bucket, subContext);
        } finally {
          // subContext.base.decref();  // OFF-HEAP
          // subContext.base = null;  // do not modify context after creation... there may be deferred execution (i.e. streaming)
        }
      }

      bucketList.add(bucket);
    }

    if (freq.missing) {
      SimpleOrderedMap<Object> missingBucket = new SimpleOrderedMap<>();
      DocSet missingDocSet = null;
      try {
        if (startTermIndex == -1) {
          addStats(missingBucket, 0);
        } else {
          missingDocSet = getFieldMissing(fcontext.searcher, fcontext.base, freq.field);
          // an extra slot was added to the end for this missing bucket
          countAcc.incrementCount(nTerms, missingDocSet.size());
          collect(missingDocSet, nTerms);
          addStats(missingBucket, nTerms);
        }

        if (freq.getSubFacets().size() > 0) {
          FacetContext subContext = fcontext.sub();
          // TODO: we can do better than this!
          if (missingDocSet == null) {
            missingDocSet = getFieldMissing(fcontext.searcher, fcontext.base, freq.field);
          }
          subContext.base = missingDocSet;
          fillBucketSubs(missingBucket, subContext);
        }

        res.add("missing", missingBucket);
      } finally {
        if (missingDocSet != null) {
          // missingDocSet.decref(); // OFF-HEAP
          missingDocSet = null;
        }
      }
    }

    return res;
  }


}


class FacetFieldProcessorFC extends FacetFieldProcessorFCBase {
  SortedDocValues sortedDocValues;


  public FacetFieldProcessorFC(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  protected BytesRef lookupOrd(int ord) throws IOException {
    return sortedDocValues.lookupOrd(ord);
  }

  protected void findStartAndEndOrds() throws IOException {
    sortedDocValues = FieldUtil.getSortedDocValues(fcontext.qcontext, sf, null);

    if (prefixRef != null) {
      startTermIndex = sortedDocValues.lookupTerm(prefixRef.get());
      if (startTermIndex < 0) startTermIndex = -startTermIndex - 1;
      prefixRef.append(UnicodeUtil.BIG_TERM);
      endTermIndex = sortedDocValues.lookupTerm(prefixRef.get());
      assert endTermIndex < 0;
      endTermIndex = -endTermIndex - 1;
    } else {
      startTermIndex = 0;
      endTermIndex = sortedDocValues.getValueCount();
    }

    // optimize collecting the "missing" bucket when startTermindex is 0 (since the "missing" ord is -1)
    startTermIndex = startTermIndex==0 && freq.missing ? -1 : startTermIndex;

    nTerms = endTermIndex - startTermIndex;
  }

  protected void collectDocs() throws IOException {
    final List<LeafReaderContext> leaves = fcontext.searcher.getIndexReader().leaves();
    final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
    LeafReaderContext ctx = null;
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    for (DocIterator docsIt = fcontext.base.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (doc >= adjustedMax) {
        do {
          ctx = ctxIt.next();
          segBase = ctx.docBase;
          segMax = ctx.reader().maxDoc();
          adjustedMax = segBase + segMax;
        } while (doc >= adjustedMax);
        assert doc >= ctx.docBase;
        setNextReader(ctx);
      }

      int term = sortedDocValues.getOrd( doc );
      int arrIdx = term - startTermIndex;
      if (arrIdx>=0 && arrIdx<nTerms) {
        countAcc.incrementCount(arrIdx, 1);
        collect(doc - segBase, arrIdx);  // per-seg collectors
        if (allBucketsSlot >= 0 && term >= 0) {
          countAcc.incrementCount(allBucketsSlot, 1);
          collect(doc - segBase, allBucketsSlot);  // per-seg collectors
        }
      }
    }
  }

}

// UnInvertedField implementation of field faceting
class FacetFieldProcessorUIF extends FacetFieldProcessorFC {
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
    prepareForCollection();

    // Minimum term docFreq in order to use the filterCache for that term.
    int defaultMinDf = Math.max(fcontext.searcher.maxDoc() >> 4, 3);  // (minimum of 3 is for test coverage purposes)
    int minDfFilterCache = freq.cacheDf == 0 ? defaultMinDf : freq.cacheDf;
    if (minDfFilterCache == -1) minDfFilterCache = Integer.MAX_VALUE;  // -1 means never cache

    docs = fcontext.base;
    fastForRandomSet = null;

    if (freq.prefix != null) {
      String indexedPrefix = sf.getType().toInternal(freq.prefix);
      startTermBytes = new BytesRef(indexedPrefix);
    }

    Fields fields = fcontext.searcher.getLeafReader().fields();
    Terms terms = fields == null ? null : fields.terms(sf.getName());


    termsEnum = null;
    deState = null;
    term = null;


    if (terms != null) {

      termsEnum = terms.iterator(null);

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
          postingsEnum = termsEnum.postings(null, postingsEnum, PostingsEnum.NONE);

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
        term = termsEnum.next();

        SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
        bucket.add("val", bucketVal);
        addStats(bucket, 0);
        if (hasSubFacets) {
          processSubs(bucket, termSet);
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

    // optimize collecting the "missing" bucket when startTermindex is 0 (since the "missing" ord is -1)
    startTermIndex = startTermIndex==0 && freq.missing ? -1 : startTermIndex;

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

      setNextReader(subCtx);

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
      collect(doc, segOrd, toGlobal);
    }
  }

  protected void collectDocs(SortedSetDocValues multiDv, DocIdSetIterator disi, LongValues toGlobal) throws IOException {
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      multiDv.setDocument(doc);
      int segOrd = (int)multiDv.nextOrd();
      collect(doc, segOrd, toGlobal); // collect anything the first time (even -1 for missing)
      if (segOrd < 0) continue;
      for(;;) {
        segOrd = (int)multiDv.nextOrd();
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
      collect(doc, arrIdx);  // per-seg collectors
      if (allBucketsSlot >= 0 && ord >= 0) {
        countAcc.incrementCount(allBucketsSlot, 1);
        collect(doc, allBucketsSlot);  // per-seg collectors
      }
    }
  }

}
