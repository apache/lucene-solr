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
package org.apache.solr.search;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;

/** @lucene.experimental */
public class DocSetUtil {

  /** The cut-off point for small sets (SortedIntDocSet) vs large sets (BitDocSet) */
  public static int smallSetSize(int maxDoc) {
    return (maxDoc>>6)+5;  // The +5 is for better test coverage for small sets
  }

  /**
   * Iterates DocSets to test for equality - slow and for testing purposes only.
   * @lucene.internal
   */
  public static boolean equals(DocSet a, DocSet b) {
    DocIterator iter1 = a.iterator();
    DocIterator iter2 = b.iterator();

    for (;;) {
      boolean n1 = iter1.hasNext();
      boolean n2 = iter2.hasNext();
      if (n1 != n2) {
        return false;
      }
      if (!n1) return true;  // made it to end
      int d1 = iter1.nextDoc();
      int d2 = iter2.nextDoc();
      if (d1 != d2) {
        return false;
      }
    }
  }

  /**
   * This variant of getDocSet will attempt to do some deduplication
   * on certain DocSets such as DocSets that match numDocs.  This means it can return
   * a cached version of the set, and the returned set should not be modified.
   * @lucene.experimental
   */
  public static DocSet getDocSet(DocSetCollector collector, SolrIndexSearcher searcher) {
    if (collector.size() == searcher.numDocs()) {
      if (!searcher.isLiveDocsInstantiated()) {
        searcher.setLiveDocs( collector.getDocSet() );
      }
      try {
        return searcher.getLiveDocSet();
      } catch (IOException e) {
        // should be impossible... liveDocs should exist, so no IO should be necessary
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    return collector.getDocSet();
  }

  /**
   * This variant of getDocSet maps all sets with size numDocs to searcher.getLiveDocs.
   * The returned set should not be modified.
   * @lucene.experimental
   */
  public static DocSet getDocSet(DocSet docs, SolrIndexSearcher searcher) {
    if (docs.size() == searcher.numDocs()) {
      if (!searcher.isLiveDocsInstantiated()) {
        searcher.setLiveDocs( docs );
      }
      try {
        // if this docset has the same cardinality as liveDocs, return liveDocs instead
        // so this set will be short lived garbage.
        return searcher.getLiveDocSet();
      } catch (IOException e) {
        // should be impossible... liveDocs should exist, so no IO should be necessary
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    return docs;
  }

  // implementers of DocSetProducer should not call this with themselves or it will result in an infinite loop
  public static DocSet createDocSet(SolrIndexSearcher searcher, Query query, DocSet filter) throws IOException {

    if (filter != null) {
      query = QueryUtils.combineQueryAndFilter(query, filter.getTopFilter());
    }

    if (query instanceof TermQuery) {
      DocSet set = createDocSet(searcher, ((TermQuery)query).getTerm() );
      // assert equals(set, createDocSetGeneric(searcher, query));
      return set;
    } else if (query instanceof DocSetProducer) {
      DocSet set = ((DocSetProducer) query).createDocSet(searcher);
      // assert equals(set, createDocSetGeneric(searcher, query));
      return set;
    }

    return createDocSetGeneric(searcher, query);
  }

  // code to produce docsets for non-docsetproducer queries
  public static DocSet createDocSetGeneric(SolrIndexSearcher searcher, Query query) throws IOException {

    int maxDoc = searcher.getIndexReader().maxDoc();
    DocSetCollector collector = new DocSetCollector(maxDoc);

    // This may throw an ExitableDirectoryReader.ExitingReaderException
    // but we should not catch it here, as we don't know how this DocSet will be used (it could be negated before use) or cached.
    searcher.search(query, collector);

    return getDocSet(collector, searcher);
  }

  public static DocSet createDocSet(SolrIndexSearcher searcher, Term term) throws IOException {
    DirectoryReader reader = searcher.getRawReader();  // raw reader to avoid extra wrapping overhead
    int maxDoc = searcher.getIndexReader().maxDoc();
    int smallSetSize = smallSetSize(maxDoc);

    String field = term.field();
    BytesRef termVal = term.bytes();

    int maxCount = 0;
    int firstReader = -1;
    List<LeafReaderContext> leaves = reader.leaves();
    PostingsEnum[] postList = new PostingsEnum[leaves.size()]; // use array for slightly higher scanning cost, but fewer memory allocations
    for (LeafReaderContext ctx : leaves) {
      assert leaves.get(ctx.ord) == ctx;
      LeafReader r = ctx.reader();
      Terms t = r.terms(field);
      if (t == null) continue;  // field is missing
      TermsEnum te = t.iterator();
      if (te.seekExact(termVal)) {
        maxCount += te.docFreq();
        postList[ctx.ord] = te.postings(null, PostingsEnum.NONE);
        if (firstReader < 0) firstReader = ctx.ord;
      }
    }

    DocSet answer = null;
    if (maxCount == 0) {
      answer = DocSet.EMPTY;
    } else if (maxCount <= smallSetSize) {
      answer = createSmallSet(leaves, postList, maxCount, firstReader);
    } else {
      answer = createBigSet(leaves, postList, maxDoc, firstReader);
    }

    return DocSetUtil.getDocSet( answer, searcher );
  }

  private static DocSet createSmallSet(List<LeafReaderContext> leaves, PostingsEnum[] postList, int maxPossible, int firstReader) throws IOException {
    int[] docs = new int[maxPossible];
    int sz = 0;
    for (int i = firstReader; i < postList.length; i++) {
      PostingsEnum postings = postList[i];
      if (postings == null) continue;
      LeafReaderContext ctx = leaves.get(i);
      Bits liveDocs = ctx.reader().getLiveDocs();
      int base = ctx.docBase;
      for (; ; ) {
        int subId = postings.nextDoc();
        if (subId == DocIdSetIterator.NO_MORE_DOCS) break;
        if (liveDocs != null && !liveDocs.get(subId)) continue;
        int globalId = subId + base;
        docs[sz++] = globalId;
      }
    }

    return new SortedIntDocSet(docs, sz);
  }


  private static DocSet createBigSet(List<LeafReaderContext> leaves, PostingsEnum[] postList, int maxDoc, int firstReader) throws IOException {
    long[] bits = new long[FixedBitSet.bits2words(maxDoc)];
    int sz = 0;
    for (int i = firstReader; i < postList.length; i++) {
      PostingsEnum postings = postList[i];
      if (postings == null) continue;
      LeafReaderContext ctx = leaves.get(i);
      Bits liveDocs = ctx.reader().getLiveDocs();
      int base = ctx.docBase;
      for (; ; ) {
        int subId = postings.nextDoc();
        if (subId == DocIdSetIterator.NO_MORE_DOCS) break;
        if (liveDocs != null && !liveDocs.get(subId)) continue;
        int globalId = subId + base;
        bits[globalId >> 6] |= (1L << globalId);
        sz++;
      }
    }

    BitDocSet docSet = new BitDocSet( new FixedBitSet(bits, maxDoc), sz );

    int smallSetSize = smallSetSize(maxDoc);
    if (sz < smallSetSize) {
      // make this optional?
      DocSet smallSet = toSmallSet( docSet );
      // assert equals(docSet, smallSet);
      return smallSet;
    }

    return docSet;
  }

  public static DocSet toSmallSet(BitDocSet bitSet) {
    int sz = bitSet.size();
    int[] docs = new int[sz];
    FixedBitSet bs = bitSet.getBits();
    int doc = -1;
    for (int i=0; i<sz; i++) {
      doc = bs.nextSetBit(doc + 1);
      docs[i] = doc;
    }
    return new SortedIntDocSet(docs);
  }

  public static void collectSortedDocSet(DocSet docs, IndexReader reader, Collector collector) throws IOException {
    // TODO add SortedDocSet sub-interface and take that.
    // TODO collectUnsortedDocSet: iterate segment, then all docSet per segment.

    final List<LeafReaderContext> leaves = reader.leaves();
    final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    LeafReaderContext ctx = null;
    LeafCollector leafCollector = null;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (doc >= adjustedMax) {
        do {
          ctx = ctxIt.next();
          segBase = ctx.docBase;
          segMax = ctx.reader().maxDoc();
          adjustedMax = segBase + segMax;
        } while (doc >= adjustedMax);
        leafCollector = collector.getLeafCollector(ctx);
      }
      if (doc < segBase) {
        throw new IllegalStateException("algorithm expects sorted DocSet but wasn't: " + docs.getClass());
      }
      leafCollector.collect(doc - segBase);  // per-seg collectors
    }
  }

}