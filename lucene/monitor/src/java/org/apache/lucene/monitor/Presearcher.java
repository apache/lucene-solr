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

import java.util.Map;
import java.util.function.BiPredicate;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * A Presearcher is used by the Monitor to reduce the number of queries actually run against a
 * Document. It defines how queries are stored in the monitor's internal index, and how a Document
 * is converted to a query against that index.
 */
public abstract class Presearcher {

  /** A Presearcher implementation that does no query filtering, and runs all registered queries */
  public static final Presearcher NO_FILTERING =
      new Presearcher() {
        @Override
        public Query buildQuery(LeafReader reader, BiPredicate<String, BytesRef> termAcceptor) {
          return new MatchAllDocsQuery();
        }

        @Override
        public Document indexQuery(Query query, Map<String, String> metadata) {
          return new Document();
        }
      };

  /**
   * Build a query for a Monitor's queryindex from a LeafReader over a set of documents to monitor.
   *
   * @param reader a {@link LeafReader} over the input documents
   * @param termAcceptor a predicate indicating if a term should be added to the query
   * @return a Query to run over a Monitor's queryindex
   */
  public abstract Query buildQuery(LeafReader reader, BiPredicate<String, BytesRef> termAcceptor);

  /**
   * Build a lucene Document to index the query in a Monitor's queryindex
   *
   * @param query the Query to index
   * @param metadata a Map of arbitrary query metadata
   * @return a lucene Document to add to the queryindex
   */
  public abstract Document indexQuery(Query query, Map<String, String> metadata);
}
