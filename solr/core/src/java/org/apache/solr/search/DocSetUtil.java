package org.apache.solr.search;

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
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/** @lucene.experimental */
public class DocSetUtil {

  private static DocSet createDocSetByIterator(SolrIndexSearcher searcher, Filter filter) throws IOException {
    int maxDoc = searcher.getIndexReader().maxDoc();

    Map fcontext = null;
    if (filter instanceof SolrFilter) {
      fcontext = ValueSource.newContext(searcher);
      ((SolrFilter) filter).createWeight(fcontext, searcher);
    }

    DocSetCollector collector = new DocSetCollector((maxDoc >> 6) + 5, maxDoc);

    for (LeafReaderContext readerContext : searcher.getIndexReader().getContext().leaves()) {
      collector.doSetNextReader(readerContext);
      Bits acceptDocs = readerContext.reader().getLiveDocs();

      DocIdSet docIdSet = filter instanceof SolrFilter
          ? ((SolrFilter) filter).getDocIdSet(fcontext, readerContext, acceptDocs)
          : filter.getDocIdSet(readerContext, acceptDocs);

      if (docIdSet == null) continue;
      DocIdSetIterator iter = docIdSet.iterator();

      for (;;) {
        int id = iter.nextDoc();
        if (id == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        collector.collect(id);
      }

    }

    return collector.getDocSet();

  }

  private static boolean equals(DocSet a, DocSet b) {
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

  // implementers of DocSetProducer should not call this with themselves or it will result in an infinite loop
  public static DocSet createDocSet(SolrIndexSearcher searcher, Query query, DocSet filter) throws IOException {

    if (filter != null) {
        Filter luceneFilter = filter.getTopFilter();
        query = new BooleanQuery.Builder()
            .add(query, BooleanClause.Occur.MUST)
            .add(luceneFilter, BooleanClause.Occur.FILTER)
            .build();
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
    DocSetCollector collector = new DocSetCollector((maxDoc >> 6) + 5, maxDoc);

    // This may throw an ExitableDirectoryReader.ExitingReaderException
    // but we should not catch it here, as we don't know how this DocSet will be used (it could be negated before use) or cached.
    searcher.search(query, collector);

    return collector.getDocSet();
  }

  public static DocSet createDocSet(SolrIndexSearcher searcher, Term term) throws IOException {
    DirectoryReader reader = searcher.getRawReader();  // raw reader to avoid extra wrapping overhead
    int maxDoc = searcher.getIndexReader().maxDoc();
    int smallSetSize = (maxDoc >> 6) + 5;


    String field = term.field();
    BytesRef termVal = term.bytes();

    int maxCount = 0;
    int firstReader = -1;
    List<LeafReaderContext> leaves = reader.leaves();
    PostingsEnum[] postList = new PostingsEnum[leaves.size()]; // use array for slightly higher scanning cost, but fewer memory allocations
    for (LeafReaderContext ctx : leaves) {
      assert leaves.get(ctx.ord) == ctx;
      LeafReader r = ctx.reader();
      Fields f = r.fields();
      Terms t = f.terms(field);
      if (t == null) continue;  // field is missing
      TermsEnum te = t.iterator();
      if (te.seekExact(termVal)) {
        maxCount += te.docFreq();
        postList[ctx.ord] = te.postings(null, PostingsEnum.NONE);
        if (firstReader < 0) firstReader = ctx.ord;
      }
    }

    if (maxCount == 0) {
      return DocSet.EMPTY;
    }

    if (maxCount <= smallSetSize) {
      return createSmallSet(leaves, postList, maxCount, firstReader);
    }

    return createBigSet(leaves, postList, maxDoc, firstReader);
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

    int smallSetSize = (maxDoc >> 6) + 5;
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

}