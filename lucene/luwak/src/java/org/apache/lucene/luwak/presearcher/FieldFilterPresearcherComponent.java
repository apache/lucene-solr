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

package org.apache.lucene.luwak.presearcher;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.EmptyTokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

/**
 * PresearcherComponent that allows you to filter queries out by a field on
 * each document.
 * <p>
 * Queries are assigned field values by passing them as part of the metadata
 * on a MonitorQuery.
 * <p>
 * N.B. DocumentBatches used with this presearcher component must all have the
 * same value in the filter field, otherwise an IllegalArgumentException will
 * be thrown
 */
public class FieldFilterPresearcherComponent extends PresearcherComponent {

  private final String field;

  /**
   * Create a new FieldFilterPresearcherComponent that filters queries on a
   * given field.
   *
   * @param field the field to filter on
   */
  public FieldFilterPresearcherComponent(String field) {
    this.field = field;
  }


  @Override
  public Query adjustPresearcherQuery(LeafReader reader, Query presearcherQuery) throws IOException {

    Query filterClause = buildFilterClause(reader);
    if (filterClause == null)
      return presearcherQuery;

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(presearcherQuery, BooleanClause.Occur.MUST);
    bq.add(filterClause, BooleanClause.Occur.FILTER);
    return bq.build();
  }

  private Query buildFilterClause(LeafReader reader) throws IOException {

    Terms terms = reader.terms(field);
    if (terms == null)
      return null;

    BooleanQuery.Builder bq = new BooleanQuery.Builder();

    int docsInBatch = reader.maxDoc();

    BytesRef term;
    TermsEnum te = terms.iterator();
    while ((term = te.next()) != null) {
      // we need to check that every document in the batch has the same field values, otherwise
      // this filtering will not work
      if (te.docFreq() != docsInBatch)
        throw new IllegalArgumentException("Some documents in this batch do not have a term value of "
            + field + ":" + Term.toString(term));
      bq.add(new TermQuery(new Term(field, BytesRef.deepCopyOf(term))), BooleanClause.Occur.SHOULD);
    }

    BooleanQuery built = bq.build();

    if (built.clauses().size() == 0)
      return null;

    return built;
  }

  @Override
  public void adjustQueryDocument(Document doc, Map<String, String> metadata) {
    if (metadata == null || !metadata.containsKey(field))
      return;
    doc.add(new TextField(field, metadata.get(field), Field.Store.YES));
  }

  @Override
  public TokenStream filterDocumentTokens(String field, TokenStream ts) {
    // We don't want tokens from this field to be present in the disjunction,
    // only in the extra filter query.  Otherwise, every doc that matches in
    // this field will be selected!
    if (this.field.equals(field))
      return new EmptyTokenStream();
    return ts;
  }
}
