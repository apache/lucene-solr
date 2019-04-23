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

import java.util.Map;
import java.util.function.BiPredicate;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.luwak.Presearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * A simple Presearcher implementation that runs all queries in a Monitor against
 * each supplied InputDocument.
 */
public class MatchAllPresearcher extends Presearcher {

  public static final Presearcher INSTANCE = new MatchAllPresearcher();

  private MatchAllPresearcher() {
    super();
  }

  @Override
  public Query buildQuery(LeafReader reader, BiPredicate<String, BytesRef> termAcceptor) {
    return new MatchAllDocsQuery();
  }

  @Override
  public Document indexQuery(Query query, Map<String, String> metadata) {
    return new Document();
  }
}
