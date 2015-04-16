package org.apache.lucene.search.spans;

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
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

/** Base class for span-based queries. */
public abstract class SpanQuery extends Query {
  /** Expert: Returns the matches for this query in an index.  
   *  Used internally to search for spans.
   *  This may return null to indicate that the SpanQuery has no results.
   */
  public abstract Spans getSpans(LeafReaderContext context, Bits acceptDocs, Map<Term,TermContext> termContexts) throws IOException;

  /**
   * Extract terms from these spans.
   * @lucene.internal
   * @see Weight#extractTerms
   */
  protected abstract void extractTerms(Set<Term> terms);

  /**
   * Returns the name of the field matched by this query.
   * <p>
   * Note that this may return null if the query matches no terms.
   */
  public abstract String getField();

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new SpanWeight(this, searcher);
  }

}
