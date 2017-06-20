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
package org.apache.lucene.search.suggest.document;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.suggest.BitsProducer;

import static org.apache.lucene.search.suggest.document.CompletionAnalyzer.HOLE_CHARACTER;
import static org.apache.lucene.search.suggest.document.CompletionAnalyzer.SEP_LABEL;

/**
 * Abstract {@link Query} that match documents containing terms with a specified prefix
 * filtered by {@link BitsProducer}. This should be used to query against any {@link SuggestField}s
 * or {@link ContextSuggestField}s of documents.
 * <p>
 * Use {@link SuggestIndexSearcher#suggest(CompletionQuery, int, boolean)} to execute any query
 * that provides a concrete implementation of this query. Example below shows using this query
 * to retrieve the top 5 documents.
 *
 * <pre class="prettyprint">
 *  SuggestIndexSearcher searcher = new SuggestIndexSearcher(reader);
 *  TopSuggestDocs suggestDocs = searcher.suggest(query, 5);
 * </pre>
 * This query rewrites to an appropriate {@link CompletionQuery} depending on the
 * type ({@link SuggestField} or {@link ContextSuggestField}) of the field the query is run against.
 *
 * @lucene.experimental
 */
public abstract class CompletionQuery extends Query {

  /**
   * Term to query against
   */
  private final Term term;

  /**
   * {@link BitsProducer} which is used to filter the document scope.
   */
  private final BitsProducer filter;

  /**
   * Creates a base Completion query against a <code>term</code>
   * with a <code>filter</code> to scope the documents
   */
  protected CompletionQuery(Term term, BitsProducer filter) {
    validate(term.text());
    this.term = term;
    this.filter = filter;
  }

  /**
   * Returns a {@link BitsProducer}. Only suggestions matching the returned
   * bits will be returned.
   */
  public BitsProducer getFilter() {
    return filter;
  }

  /**
   * Returns the field name this query should
   * be run against
   */
  public String getField() {
    return term.field();
  }

  /**
   * Returns the term to be queried against
   */
  public Term getTerm() {
    return term;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    byte type = 0;
    boolean first = true;
    Terms terms;
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader leafReader = context.reader();
      try {
        if ((terms = leafReader.terms(getField())) == null) {
          continue;
        }
      } catch (IOException e) {
        continue;
      }
      if (terms instanceof CompletionTerms) {
        CompletionTerms completionTerms = (CompletionTerms) terms;
        byte t = completionTerms.getType();
        if (first) {
          type = t;
          first = false;
        } else if (type != t) {
          throw new IllegalStateException(getField() + " has values of multiple types");
        }
      }
    }

    if (first == false) {
      if (this instanceof ContextQuery) {
        if (type == SuggestField.TYPE) {
          throw new IllegalStateException(this.getClass().getSimpleName()
              + " can not be executed against a non context-enabled SuggestField: "
              + getField());
        }
      } else {
        if (type == ContextSuggestField.TYPE) {
          return new ContextQuery(this);
        }
      }
    }
    return super.rewrite(reader);
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append(term.text());
    buffer.append('*');
    if (filter != null) {
      buffer.append(",");
      buffer.append("filter");
      buffer.append(":");
      buffer.append(filter.toString());
    }
    return buffer.toString();
  }

  private void validate(String termText) {
    for (int i = 0; i < termText.length(); i++) {
      switch (termText.charAt(i)) {
        case HOLE_CHARACTER:
          throw new IllegalArgumentException(
              "Term text cannot contain HOLE character U+001E; this character is reserved");
        case SEP_LABEL:
          throw new IllegalArgumentException(
              "Term text cannot contain unit separator character U+001F; this character is reserved");
        default:
          break;
      }
    }
  }
}
