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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.index.SlowCompositeReaderWrapper;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.facet.SweepCountAware.SegCountGlobal;
import org.apache.solr.search.facet.SlotAcc.CountSlotAcc;
import org.apache.solr.search.facet.SlotAcc.SlotContext;
import org.apache.solr.search.facet.SlotAcc.SweepCountAccStruct;
import org.apache.solr.search.facet.SlotAcc.SweepingCountSlotAcc;
import org.apache.solr.search.facet.SweepDocIterator.SweepIteratorAndCounts;
import org.apache.solr.uninverting.DocTermOrds;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Final form of the un-inverted field:
 *   Each document points to a list of term numbers that are contained in that document.
 *
 *   Term numbers are in sorted order, and are encoded as variable-length deltas from the
 *   previous term number.  Real term numbers start at 2 since 0 and 1 are reserved.  A
 *   term number of 0 signals the end of the termNumber list.
 *
 *   There is a single int[maxDoc()] which either contains a pointer into a byte[] for
 *   the termNumber lists, or directly contains the termNumber list if it fits in the 4
 *   bytes of an integer.  If the first byte in the integer is 1, the next 3 bytes
 *   are a pointer into a byte[] where the termNumber list starts.
 *
 *   There are actually 256 byte arrays, to compensate for the fact that the pointers
 *   into the byte arrays are only 3 bytes long.  The correct byte array for a document
 *   is a function of its id.
 *
 *   To save space and speed up faceting, any term that matches enough documents will
 *   not be un-inverted... it will be skipped while building the un-inverted field structure,
 *   and will use a set intersection method during faceting.
 *
 *   To further save memory, the terms (the actual string values) are not all stored in
 *   memory, but a TermIndex is used to convert term numbers to term values only
 *   for the terms needed after faceting has completed.  Only every 128th term value
 *   is stored, along with its corresponding term number, and this is used as an
 *   index to find the closest term and iterate until the desired number is hit (very
 *   much like Lucene's own internal term index).
 *
 */
public class UnInvertedField extends DocTermOrds {
  private static int TNUM_OFFSET=2;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static class TopTerm {
    Query termQuery;
    BytesRef term;
    int termNum;

    long memSize() {
      return 8 +   // obj header
          8 + 8 +term.length +  //term
          4;    // int
    }
  }

  long memsz;
  final AtomicLong use = new AtomicLong(); // number of uses

  /* The number of documents holding the term {@code maxDocs = maxTermCounts[termNum]}. */
  int[] maxTermCounts = new int[1024];

  /* termNum -> docIDs for big terms. */
  final Map<Integer,TopTerm> bigTerms = new LinkedHashMap<>();

  private SolrIndexSearcher.DocsEnumState deState;
  private final SolrIndexSearcher searcher;

  private static final UnInvertedField uifPlaceholder = new UnInvertedField();

  private UnInvertedField() { // Dummy for synchronization.
    super("fake", 0, 0); // cheapest initialization I can find.
    searcher = null;
  }

  /**
   * Called for each term in the field being uninverted.
   * Collects {@link #maxTermCounts} for all bigTerms as well as storing them in {@link #bigTerms}.
   * @param te positioned at the current term.
   * @param termNum the ID/pointer/ordinal of the current term. Monotonically increasing between calls.
   */
  @Override
  protected void visitTerm(TermsEnum te, int termNum) throws IOException {

    if (termNum >= maxTermCounts.length) {
      // resize by doubling - for very large number of unique terms, expanding
      // by 4K and resultant GC will dominate uninvert times.  Resize at end if material
      int[] newMaxTermCounts = new int[ Math.min(Integer.MAX_VALUE-16, maxTermCounts.length*2) ];
      System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, termNum);
      maxTermCounts = newMaxTermCounts;
    }

    final BytesRef term = te.term();

    if (te.docFreq() > maxTermDocFreq) {
      Term t = new Term(field, term);  // this makes a deep copy of the term bytes
      TopTerm topTerm = new TopTerm();
      topTerm.term = t.bytes();
      topTerm.termNum = termNum;
      topTerm.termQuery = new TermQuery(t);

      bigTerms.put(topTerm.termNum, topTerm);

      if (deState == null) {
        deState = new SolrIndexSearcher.DocsEnumState();
        deState.fieldName = field;
        deState.liveDocs = searcher.getLiveDocsBits();
        deState.termsEnum = te;  // TODO: check for MultiTermsEnum in SolrIndexSearcher could now fail?
        deState.postingsEnum = postingsEnum;
        deState.minSetSizeCached = maxTermDocFreq;
      }

      postingsEnum = deState.postingsEnum;
      DocSet set = searcher.getDocSet(deState);
      maxTermCounts[termNum] = set.size();
    }
  }

  @Override
  protected void setActualDocFreq(int termNum, int docFreq) {
    maxTermCounts[termNum] = docFreq;
  }

  public long memSize() {
    // can cache the mem size since it shouldn't change
    if (memsz!=0) return memsz;
    long sz = super.ramBytesUsed();
    sz += 8*8 + 32; // local fields
    sz += bigTerms.size() * 64;
    for (TopTerm tt : bigTerms.values()) {
      sz += tt.memSize();
    }
    if (maxTermCounts != null)
      sz += maxTermCounts.length * 4;
    memsz = sz;
    return sz;
  }

  public UnInvertedField(String field, SolrIndexSearcher searcher) throws IOException {
    super(field,
        // threshold, over which we use set intersections instead of counting
        // to (1) save memory, and (2) speed up faceting.
        // Add 2 for testing purposes so that there will always be some terms under
        // the threshold even when the index is very
        // small.
        searcher.maxDoc()/20 + 2,
        DEFAULT_INDEX_INTERVAL_BITS);

    assert TestInjection.injectUIFOutOfMemoryError();

    final String prefix = TrieField.getMainValuePrefix(searcher.getSchema().getFieldType(field));
    this.searcher = searcher;
    try {
      // TODO: it's wasteful to create one of these each time
      // but DocTermOrds will throw an exception if it thinks the field has doc values (which is faked by UnInvertingReader)
      LeafReader r = SlowCompositeReaderWrapper.wrap(searcher.getRawReader());
      uninvert(r, r.getLiveDocs(), prefix == null ? null : new BytesRef(prefix));
    } catch (IllegalStateException ise) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ise);
    }
    if (tnums != null) {
      for(byte[] target : tnums) {
        if (target != null && target.length > (1<<24)*.9) {
          log.warn("Approaching too many values for UnInvertedField faceting on field '{}' : bucket size={}", field, target.length);
        }
      }
    }

    // free space if outrageously wasteful (tradeoff memory/cpu) 
    if ((maxTermCounts.length - numTermsInField) > 1024) { // too much waste!
      int[] newMaxTermCounts = new int[numTermsInField];
      System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, numTermsInField);
      maxTermCounts = newMaxTermCounts;
    }

    log.info("UnInverted multi-valued field {}", this);
    //System.out.println("CREATED: " + toString() + " ti.index=" + ti.index);
  }

  public int getNumTerms() {
    return numTermsInField;
  }



  public class DocToTerm implements Closeable {
    private final DocSet[] bigTermSets;
    private final int[] bigTermNums;
    private TermsEnum te;

    public DocToTerm() throws IOException {
      bigTermSets = new DocSet[bigTerms.size()];
      bigTermNums = new int[bigTerms.size()];
      int i=0;
      for (TopTerm tt : bigTerms.values()) {
        bigTermSets[i] = searcher.getDocSet(tt.termQuery);
        bigTermNums[i] = tt.termNum;
        i++;
      }
    }

    public BytesRef lookupOrd(int ord) throws IOException {
      return getTermValue( getTermsEnum() , ord );
    }

    public TermsEnum getTermsEnum() throws IOException {
      if (te == null) {
        te = getOrdTermsEnum(searcher.getSlowAtomicReader());
      }
      return te;
    }

    public void getBigTerms(int doc, Callback target) throws IOException {
      if (bigTermSets != null) {
        for (int i=0; i<bigTermSets.length; i++) {
          if (bigTermSets[i].exists(doc)) {
            target.call( bigTermNums[i] );
          }
        }
      }
    }

    public void getSmallTerms(int doc, Callback target) {
      if (termInstances > 0) {
        int code = index[doc];

        if ((code & 0x80000000)!=0) {
          int pos = code & 0x7fffffff;
          int whichArray = (doc >>> 16) & 0xff;
          byte[] arr = tnums[whichArray];
          int tnum = 0;
          for(;;) {
            int delta = 0;
            for(;;) {
              byte b = arr[pos++];
              delta = (delta << 7) | (b & 0x7f);
              if ((b & 0x80) == 0) break;
            }
            if (delta == 0) break;
            tnum += delta - TNUM_OFFSET;
            target.call(tnum);
          }
        } else {
          int tnum = 0;
          int delta = 0;
          for (;;) {
            delta = (delta << 7) | (code & 0x7f);
            if ((code & 0x80)==0) {
              if (delta==0) break;
              tnum += delta - TNUM_OFFSET;
              target.call(tnum);
              delta = 0;
            }
            code >>>= 8;
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      for (DocSet set : bigTermSets) {
        // set.decref(); // OFF-HEAP
      }
    }
  }

  public interface Callback {
    public void call(int termNum);
  }



  private void getCounts(FacetFieldProcessorByArrayUIF processor) throws IOException {
    DocSet docs = processor.fcontext.base;
    int baseSize = docs.size();
    int maxDoc = searcher.maxDoc();

    // what about allBuckets?
    if (baseSize < processor.effectiveMincount) {
      return;
    }

    SweepCountAccStruct baseCountAccStruct = SweepingCountSlotAcc.baseStructOf(processor);
    final List<SweepCountAccStruct> others = SweepingCountSlotAcc.otherStructsOf(processor);

    final int[] index = this.index;

    boolean doNegative = baseSize > maxDoc >> 1 && termInstances > 0 && docs instanceof BitDocSet && baseCountAccStruct != null;

    if (doNegative) {
      FixedBitSet bs = ((BitDocSet) docs).getBits().clone();
      bs.flip(0, maxDoc);
      // TODO: when iterator across negative elements is available, use that
      // instead of creating a new bitset and inverting.
      docs = new BitDocSet(bs, maxDoc - baseSize);
      // simply negating will mean that we have deleted docs in the set.
      // that should be OK, as their entries in our table should be empty.
      baseCountAccStruct = new SweepCountAccStruct(baseCountAccStruct, docs);
    }

    // For the biggest terms, do straight set intersections
    for (TopTerm tt : bigTerms.values()) {
      // TODO: counts could be deferred if sorting by index order
      final int termOrd = tt.termNum;
      Iterator<SweepCountAccStruct> othersIter = others.iterator();
      SweepCountAccStruct entry = baseCountAccStruct != null ? baseCountAccStruct : othersIter.next();
      for (;;) {
        entry.countAcc.incrementCount(termOrd, searcher.numDocs(tt.termQuery, entry.docSet));
        if (!othersIter.hasNext()) {
          break;
        }
        entry = othersIter.next();
      }
    }

    // TODO: we could short-circuit counting altogether for sorted faceting
    // where we already have enough terms from the bigTerms

    if (termInstances > 0) {
      final SweepIteratorAndCounts iterAndCounts = SweepDocIterator.newInstance(baseCountAccStruct, others);
      final SweepDocIterator iter = iterAndCounts.iter;
      final SegCountGlobal counts = new SegCountGlobal(iterAndCounts.countAccs);
      while (iter.hasNext()) {
        int doc = iter.nextDoc();
        int maxIdx = iter.registerCounts(counts);
        int code = index[doc];

        if ((code & 0x80000000)!=0) {
          int pos = code & 0x7fffffff;
          int whichArray = (doc >>> 16) & 0xff;
          byte[] arr = tnums[whichArray];
          int tnum = 0;
          for (; ; ) {
            int delta = 0;
            for (; ; ) {
              byte b = arr[pos++];
              delta = (delta << 7) | (b & 0x7f);
              if ((b & 0x80) == 0) break;
            }
            if (delta == 0) break;
            tnum += delta - TNUM_OFFSET;
            counts.incrementCount(tnum, 1, maxIdx);
          }
        } else {
          int tnum = 0;
          int delta = 0;
          for (; ; ) {
            delta = (delta << 7) | (code & 0x7f);
            if ((code & 0x80) == 0) {
              if (delta == 0) break;
              tnum += delta - TNUM_OFFSET;
              counts.incrementCount(tnum, 1, maxIdx);
              delta = 0;
            }
            code >>>= 8;
          }
        }
      }
    }

    if (doNegative) {
      final CountSlotAcc baseCounts = processor.countAcc;
      for (int i=0; i<numTermsInField; i++) {
 //       counts[i] = maxTermCounts[i] - counts[i];
        baseCounts.incrementCount(i, maxTermCounts[i] - (int) baseCounts.getCount(i)*2);
      }
    }

    /*** TODO - future optimization to handle allBuckets
    if (processor.allBucketsSlot >= 0) {
      int all = 0;  // overflow potential
      for (int i=0; i<numTermsInField; i++) {
        all += counts.getCount(i);
      }
      counts.incrementCount(processor.allBucketsSlot, all);
    }
     ***/
  }



  public void collectDocs(FacetFieldProcessorByArrayUIF processor) throws IOException {
    if (processor.collectAcc==null && processor.allBucketsAcc == null && processor.startTermIndex == 0 && processor.endTermIndex >= numTermsInField) {
      getCounts(processor);
      return;
    }

    collectDocsGeneric(processor);
  }

  // called from FieldFacetProcessor
  // TODO: do a callback version that can be specialized!
  public void collectDocsGeneric(FacetFieldProcessorByArrayUIF processor) throws IOException {
    use.incrementAndGet();

    int startTermIndex = processor.startTermIndex;
    int endTermIndex = processor.endTermIndex;
    int nTerms = processor.nTerms;
    DocSet docs = processor.fcontext.base;

    int uniqueTerms = 0;
    final CountSlotAcc countAcc = processor.countAcc;
    final SweepCountAccStruct baseCountAccStruct = SweepingCountSlotAcc.baseStructOf(processor);
    final List<SweepCountAccStruct> others = SweepingCountSlotAcc.otherStructsOf(processor);

    for (TopTerm tt : bigTerms.values()) {
      if (tt.termNum >= startTermIndex && tt.termNum < endTermIndex) {
        // handle the biggest terms
        DocSet termSet = searcher.getDocSet(tt.termQuery);
        DocSet intersection = termSet.intersection(docs);
        int collected = processor.collectFirstPhase(intersection, tt.termNum - startTermIndex,
                                                    slotNum -> { return new SlotContext(tt.termQuery); });
        final int termOrd = tt.termNum - startTermIndex;
        countAcc.incrementCount(termOrd, collected);
        for (SweepCountAccStruct entry : others) {
          entry.countAcc.incrementCount(termOrd, termSet.intersectionSize(entry.docSet));
        }
        if (collected > 0) {
          uniqueTerms++;
        }
      }
    }


    if (termInstances > 0) {

      final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
      final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
      LeafReaderContext ctx = null;
      int segBase = 0;
      int segMax;
      int adjustedMax = 0;


      // TODO: handle facet.prefix here!!!

      SweepIteratorAndCounts sweepIterAndCounts = SweepDocIterator.newInstance(baseCountAccStruct, others);
      final SweepDocIterator iter = sweepIterAndCounts.iter;
      final CountSlotAcc[] countAccs = sweepIterAndCounts.countAccs;
      final SegCountGlobal counts = new SegCountGlobal(countAccs);
      while (iter.hasNext()) {
        int doc = iter.nextDoc();
        int maxIdx = iter.registerCounts(counts);
        boolean collectBase = iter.collectBase();

        if (doc >= adjustedMax) {
          do {
            ctx = ctxIt.next();
            if (ctx == null) {
              // should be impossible
              throw new RuntimeException("INTERNAL FACET ERROR");
            }
            segBase = ctx.docBase;
            segMax = ctx.reader().maxDoc();
            adjustedMax = segBase + segMax;
          } while (doc >= adjustedMax);
          assert doc >= ctx.docBase;
          processor.setNextReaderFirstPhase(ctx);
        }
        int segDoc = doc - segBase;


        int code = index[doc];

        if ((code & 0x80000000)!=0) {
          int pos = code & 0x7fffffff;
          int whichArray = (doc >>> 16) & 0xff;
          byte[] arr = tnums[whichArray];
          int tnum = 0;
          for(;;) {
            int delta = 0;
            for(;;) {
              byte b = arr[pos++];
              delta = (delta << 7) | (b & 0x7f);
              if ((b & 0x80) == 0) break;
            }
            if (delta == 0) break;
            tnum += delta - TNUM_OFFSET;
            int arrIdx = tnum - startTermIndex;
            if (arrIdx < 0) continue;
            if (arrIdx >= nTerms) break;
            counts.incrementCount(arrIdx, 1, maxIdx);
            if (collectBase) {
              processor.collectFirstPhase(segDoc, arrIdx, processor.slotContext);
            }
          }
        } else {
          int tnum = 0;
          int delta = 0;
          for (;;) {
            delta = (delta << 7) | (code & 0x7f);
            if ((code & 0x80)==0) {
              if (delta==0) break;
              tnum += delta - TNUM_OFFSET;
              int arrIdx = tnum - startTermIndex;
              if (arrIdx >= 0) {
                if (arrIdx >= nTerms) break;
                counts.incrementCount(arrIdx, 1, maxIdx);
                if (collectBase) {
                  processor.collectFirstPhase(segDoc, arrIdx, processor.slotContext);
                }
              }
              delta = 0;
            }
            code >>>= 8;
          }
        }
      }
    }


  }



  String getReadableValue(BytesRef termval, FieldType ft, CharsRefBuilder charsRef) {
    return ft.indexedToReadable(termval, charsRef).toString();
  }

  /** may return a reused BytesRef */
  BytesRef getTermValue(TermsEnum te, int termNum) throws IOException {
    //System.out.println("getTermValue termNum=" + termNum + " this=" + this + " numTerms=" + numTermsInField);
    if (bigTerms.size() > 0) {
      // see if the term is one of our big terms.
      TopTerm tt = bigTerms.get(termNum);
      if (tt != null) {
        //System.out.println("  return big " + tt.term);
        return tt.term;
      }
    }

    return lookupTerm(te, termNum);
  }

  @Override
  public String toString() {
    final long indexSize = indexedTermsArray == null ? 0 : (8+8+8+8+(indexedTermsArray.length<<3)+sizeOfIndexedStrings); // assume 8 byte references?
    return "{field=" + field
        + ",memSize="+memSize()
        + ",tindexSize="+indexSize
        + ",time="+total_time
        + ",phase1="+phase1_time
        + ",nTerms="+numTermsInField
        + ",bigTerms="+bigTerms.size()
        + ",termInstances="+termInstances
        + ",uses="+use.get()
        + "}";
  }

  //////////////////////////////////////////////////////////////////
  //////////////////////////// caching /////////////////////////////
  //////////////////////////////////////////////////////////////////

  @SuppressWarnings("unchecked")
  public static UnInvertedField getUnInvertedField(String field, SolrIndexSearcher searcher) throws IOException {
    SolrCache<String, UnInvertedField> cache = searcher.getFieldValueCache();
    if (cache == null) {
      return new UnInvertedField(field, searcher);
    }
    return cache.computeIfAbsent(field, f -> new UnInvertedField(f, searcher));
  }

  // Returns null if not already populated
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static UnInvertedField checkUnInvertedField(String field, SolrIndexSearcher searcher) throws IOException {
    SolrCache cache = searcher.getFieldValueCache();
    if (cache == null) {
      return null;
    }
    Object uif = cache.get(field);  // cache is already synchronized, so no extra sync needed
    // placeholder is an implementation detail, keep it hidden and return null if that is what we got
    return uif==uifPlaceholder || !(uif instanceof UnInvertedField)? null : (UnInvertedField) uif;
  }

}
