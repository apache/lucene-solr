package org.apache.lucene.search.payloads;

import org.apache.lucene.search.BooleanClause;
/**
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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.PayloadSpans;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

/**
 * Experimental class to get set of payloads for most standard Lucene queries.
 * Operates like Highlighter - IndexReader should only contain doc of interest,
 * best to use MemoryIndex.
 *
 * <p/>
 * <font color="#FF0000">
   * WARNING: The status of the <b>Payloads</b> feature is experimental.
   * The APIs introduced here might change in the future and will not be
   * supported anymore in such a case.</font>
 * 
 */
public class PayloadSpanUtil {
  private IndexReader reader;

  /**
   * @param reader
   *          that contains doc with payloads to extract
   */
  public PayloadSpanUtil(IndexReader reader) {
    this.reader = reader;
  }

  /**
   * Query should be rewritten for wild/fuzzy support.
   * 
   * @param query
   * @return payloads Collection
   * @throws IOException
   */
  public Collection getPayloadsForQuery(Query query) throws IOException {
    Collection payloads = new ArrayList();
    queryToSpanQuery(query, payloads);
    return payloads;
  }

  private void queryToSpanQuery(Query query, Collection payloads)
      throws IOException {
    if (query instanceof BooleanQuery) {
      BooleanClause[] queryClauses = ((BooleanQuery) query).getClauses();

      for (int i = 0; i < queryClauses.length; i++) {
        if (!queryClauses[i].isProhibited()) {
          queryToSpanQuery(queryClauses[i].getQuery(), payloads);
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
      sp.setBoost(query.getBoost());
      getPayloads(payloads, sp);
    } else if (query instanceof TermQuery) {
      SpanTermQuery stq = new SpanTermQuery(((TermQuery) query).getTerm());
      stq.setBoost(query.getBoost());
      getPayloads(payloads, stq);
    } else if (query instanceof SpanQuery) {
      getPayloads(payloads, (SpanQuery) query);
    } else if (query instanceof FilteredQuery) {
      queryToSpanQuery(((FilteredQuery) query).getQuery(), payloads);
    } else if (query instanceof DisjunctionMaxQuery) {

      for (Iterator iterator = ((DisjunctionMaxQuery) query).iterator(); iterator
          .hasNext();) {
        queryToSpanQuery((Query) iterator.next(), payloads);
      }

    } else if (query instanceof MultiPhraseQuery) {
      final MultiPhraseQuery mpq = (MultiPhraseQuery) query;
      final List termArrays = mpq.getTermArrays();
      final int[] positions = mpq.getPositions();
      if (positions.length > 0) {

        int maxPosition = positions[positions.length - 1];
        for (int i = 0; i < positions.length - 1; ++i) {
          if (positions[i] > maxPosition) {
            maxPosition = positions[i];
          }
        }

        final List[] disjunctLists = new List[maxPosition + 1];
        int distinctPositions = 0;

        for (int i = 0; i < termArrays.size(); ++i) {
          final Term[] termArray = (Term[]) termArrays.get(i);
          List disjuncts = disjunctLists[positions[i]];
          if (disjuncts == null) {
            disjuncts = (disjunctLists[positions[i]] = new ArrayList(
                termArray.length));
            ++distinctPositions;
          }
          for (int j = 0; j < termArray.length; ++j) {
            disjuncts.add(new SpanTermQuery(termArray[j]));
          }
        }

        int positionGaps = 0;
        int position = 0;
        final SpanQuery[] clauses = new SpanQuery[distinctPositions];
        for (int i = 0; i < disjunctLists.length; ++i) {
          List disjuncts = disjunctLists[i];
          if (disjuncts != null) {
            clauses[position++] = new SpanOrQuery((SpanQuery[]) disjuncts
                .toArray(new SpanQuery[disjuncts.size()]));
          } else {
            ++positionGaps;
          }
        }

        final int slop = mpq.getSlop();
        final boolean inorder = (slop == 0);

        SpanNearQuery sp = new SpanNearQuery(clauses, slop + positionGaps,
            inorder);
        sp.setBoost(query.getBoost());
        getPayloads(payloads, sp);
      }
    }
  }

  private void getPayloads(Collection payloads, SpanQuery query)
      throws IOException {
    PayloadSpans spans = query.getPayloadSpans(reader);

    while (spans.next() == true) {
      if (spans.isPayloadAvailable()) {
        Collection payload = spans.getPayload();
        Iterator it = payload.iterator();
        while (it.hasNext()) {
          byte[] bytes = (byte[]) it.next();
          payloads.add(bytes);
        }

      }
    }
  }
}
