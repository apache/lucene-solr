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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.Query;

/**
 * Builds a {@link QueryTree} for a query that needs custom treatment
 *
 * The default query analyzers will use the QueryVisitor API to extract
 * terms from queries.  If different handling is needed, implement a
 * CustomQueryHandler and pass it to the presearcher
 */
public interface CustomQueryHandler {

  /**
   * Builds a {@link QueryTree} node from a query
   */
  QueryTree handleQuery(Query query, TermWeightor termWeightor);

  /**
   * Adds additional processing to the {@link TokenStream} over a document's
   * terms index
   */
  default TokenStream wrapTermStream(String field, TokenStream in) {
    return in;
  }

}
