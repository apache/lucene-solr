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

package org.apache.lucene.monitor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * A TermFilteredPresearcher that indexes queries multiple times, with terms collected
 * from different routes through a querytree.  Each route will produce a set of terms
 * that are *sufficient* to select the query, and are indexed into a separate, suffixed field.
 * <p>
 * Incoming documents are then converted to a set of Disjunction queries over each
 * suffixed field, and these queries are combined into a conjunction query, such that the
 * document's set of terms must match a term from each route.
 * <p>
 * This allows filtering out of documents that contain one half of a two-term phrase query, for
 * example.  The query {@code "hello world"} will be indexed twice, once under 'hello' and once
 * under 'world'.  A document containing the terms "hello there" would match the first field,
 * but not the second, and so would not be selected for matching.
 * <p>
 * The number of passes the presearcher makes is configurable.  More passes will improve the
 * selected/matched ratio, but will take longer to index and will use more RAM.
 * <p>
 * A minimum weight can we set for terms to be chosen for the second and subsequent passes.  This
 * allows users to avoid indexing stopwords, for example.
 */
public class MultipassTermFilteredPresearcher extends TermFilteredPresearcher {

  private final int passes;
  private final float minWeight;

  /**
   * Construct a new MultipassTermFilteredPresearcher
   *
   * @param passes        the number of times a query should be indexed
   * @param minWeight     the minimum weight a querytree should be advanced over
   * @param weightor      the TreeWeightor to use
   * @param queryHandlers a list of custom query handlers
   * @param filterFields  a set of fields to use as filters
   */
  public MultipassTermFilteredPresearcher(int passes, float minWeight, TermWeightor weightor,
                                          List<CustomQueryHandler> queryHandlers, Set<String> filterFields) {
    super(weightor, queryHandlers, filterFields);
    this.passes = passes;
    this.minWeight = minWeight;
  }

  /**
   * Construct a new MultipassTermFilteredPresearcher using {@link TermFilteredPresearcher#DEFAULT_WEIGHTOR}
   * <p>
   * Note that this will be constructed with a minimum advance weight of zero
   *
   * @param passes     the number of times a query should be indexed
   */
  public MultipassTermFilteredPresearcher(int passes) {
    this(passes, 0, DEFAULT_WEIGHTOR, Collections.emptyList(), Collections.emptySet());
  }

  @Override
  protected DocumentQueryBuilder getQueryBuilder() {
    return new MultipassDocumentQueryBuilder();
  }

  private static String field(String field, int pass) {
    return field + "_" + pass;
  }

  private class MultipassDocumentQueryBuilder implements DocumentQueryBuilder {

    BooleanQuery.Builder[] queries = new BooleanQuery.Builder[passes];
    Map<String, BytesRefHash> terms = new HashMap<>();

    MultipassDocumentQueryBuilder() {
      for (int i = 0; i < queries.length; i++) {
        queries[i] = new BooleanQuery.Builder();
      }
    }

    @Override
    public void addTerm(String field, BytesRef term) {
      BytesRefHash t = terms.computeIfAbsent(field, f -> new BytesRefHash());
      t.add(term);
    }

    @Override
    public Query build() {
      Map<String, BytesRef[]> collectedTerms = new HashMap<>();
      for (Map.Entry<String, BytesRefHash> entry : terms.entrySet()) {
        collectedTerms.put(entry.getKey(), convertHash(entry.getValue()));
      }
      BooleanQuery.Builder parent = new BooleanQuery.Builder();
      for (int i = 0; i < passes; i++) {
        BooleanQuery.Builder child = new BooleanQuery.Builder();
        for (String field : terms.keySet()) {
          child.add(new TermInSetQuery(field(field, i), collectedTerms.get(field)), BooleanClause.Occur.SHOULD);
        }
        parent.add(child.build(), BooleanClause.Occur.MUST);
      }
      return parent.build();
    }
  }

  @Override
  public Document buildQueryDocument(QueryTree querytree) {

    Document doc = new Document();

    for (int i = 0; i < passes; i++) {
      Map<String, BytesRefHash> fieldTerms = collectTerms(querytree);
      for (Map.Entry<String, BytesRefHash> entry : fieldTerms.entrySet()) {
        // we add the index terms once under a suffixed field for the multipass query, and
        // once under the plan field name for the TermsEnumTokenFilter
        doc.add(new Field(field(entry.getKey(), i),
            new TermsEnumTokenStream(new BytesRefHashIterator(entry.getValue())), QUERYFIELDTYPE));
        doc.add(new Field(entry.getKey(),
            new TermsEnumTokenStream(new BytesRefHashIterator(entry.getValue())), QUERYFIELDTYPE));
      }
      querytree.advancePhase(minWeight);
    }

    return doc;
  }

  private static BytesRef[] convertHash(BytesRefHash hash) {
    BytesRef[] terms = new BytesRef[hash.size()];
    for (int i = 0; i < terms.length; i++) {
      BytesRef t = new BytesRef();
      terms[i] = hash.get(i, t);
    }
    return terms;
  }

}
