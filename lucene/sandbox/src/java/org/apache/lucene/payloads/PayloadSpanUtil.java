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
package org.apache.lucene.payloads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;

/**
 * Experimental class to get set of payloads for most standard Lucene queries.
 * Operates like Highlighter - IndexReader should only contain doc of interest,
 * best to use MemoryIndex.
 *
 * @lucene.experimental
 * 
 */
public class PayloadSpanUtil {
  private IndexReaderContext context;

  /**
   * @param context
   *          that contains doc with payloads to extract
   *          
   * @see IndexReader#getContext()
   */
  public PayloadSpanUtil(IndexReaderContext context) {
    this.context = context;
  }

  /**
   * Query should be rewritten for wild/fuzzy support.
   * 
   * @param query rewritten query
   * @return payloads Collection
   * @throws IOException if there is a low-level I/O error
   */
  public Collection<byte[]> getPayloadsForQuery(Query query) throws IOException {
    Collection<byte[]> payloads = new ArrayList<>();
    queryToSpanQuery(query, payloads);
    return payloads;
  }

  private void queryToSpanQuery(Query query, Collection<byte[]> payloads)
      throws IOException {
    if (query instanceof BooleanQuery) {
      for (BooleanClause clause : (BooleanQuery) query) {
        if (!clause.isProhibited()) {
          queryToSpanQuery(clause.getQuery(), payloads);
        }
      }

    } else if (query instanceof PhraseQuery) {
      Term[] phraseQueryTerms = ((PhraseQuery) query).getTerms();
      SpanQuery[] clauses = new SpanQuery[phraseQueryTerms.length];
      for (int i = 0; i < phraseQueryTerms.length; i++) {
        clauses[i] = new SpanTermQuery(phraseQueryTerms[i]);
      }

      int slop = ((PhraseQuery) query).getSlop();
      boolean inorder = false;

      if (slop == 0) {
        inorder = true;
      }

      SpanNearQuery sp = new SpanNearQuery(clauses, slop, inorder);
      getPayloads(payloads, sp);
    } else if (query instanceof TermQuery) {
      SpanTermQuery stq = new SpanTermQuery(((TermQuery) query).getTerm());
      getPayloads(payloads, stq);
    } else if (query instanceof SpanQuery) {
      getPayloads(payloads, (SpanQuery) query);
    } else if (query instanceof DisjunctionMaxQuery) {

      for (Iterator<Query> iterator = ((DisjunctionMaxQuery) query).iterator(); iterator
          .hasNext();) {
        queryToSpanQuery(iterator.next(), payloads);
      }

    } else if (query instanceof MultiPhraseQuery) {
      final MultiPhraseQuery mpq = (MultiPhraseQuery) query;
      final Term[][] termArrays = mpq.getTermArrays();
      final int[] positions = mpq.getPositions();
      if (positions.length > 0) {

        int maxPosition = positions[positions.length - 1];
        for (int i = 0; i < positions.length - 1; ++i) {
          if (positions[i] > maxPosition) {
            maxPosition = positions[i];
          }
        }

        @SuppressWarnings({"rawtypes","unchecked"}) final List<Query>[] disjunctLists =
            new List[maxPosition + 1];
        int distinctPositions = 0;

        for (int i = 0; i < termArrays.length; ++i) {
          final Term[] termArray = termArrays[i];
          List<Query> disjuncts = disjunctLists[positions[i]];
          if (disjuncts == null) {
            disjuncts = (disjunctLists[positions[i]] = new ArrayList<>(
                termArray.length));
            ++distinctPositions;
          }
          for (final Term term : termArray) {
            disjuncts.add(new SpanTermQuery(term));
          }
        }

        int positionGaps = 0;
        int position = 0;
        final SpanQuery[] clauses = new SpanQuery[distinctPositions];
        for (int i = 0; i < disjunctLists.length; ++i) {
          List<Query> disjuncts = disjunctLists[i];
          if (disjuncts != null) {
            clauses[position++] = new SpanOrQuery(disjuncts
                .toArray(new SpanQuery[disjuncts.size()]));
          } else {
            ++positionGaps;
          }
        }

        final int slop = mpq.getSlop();
        final boolean inorder = (slop == 0);

        SpanNearQuery sp = new SpanNearQuery(clauses, slop + positionGaps,
                                                      inorder);
        getPayloads(payloads, sp);
      }
    }
  }

  private void getPayloads(Collection<byte []> payloads, SpanQuery query)
      throws IOException {

    final IndexSearcher searcher = new IndexSearcher(context);
    searcher.setQueryCache(null);

    query = (SpanQuery) searcher.rewrite(query);
    SpanWeight w = (SpanWeight) searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1);

    PayloadSpanCollector collector = new PayloadSpanCollector();
    for (LeafReaderContext leafReaderContext : context.leaves()) {
      final Spans spans = w.getSpans(leafReaderContext, SpanWeight.Postings.PAYLOADS);
      if (spans != null) {
        while (spans.nextDoc() != Spans.NO_MORE_DOCS) {
          while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
            collector.reset();
            spans.collect(collector);
            payloads.addAll(collector.getPayloads());
          }
        }
      }
    }
  }
}
