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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.LuceneTestCase;

public abstract class MonitorTestBase extends LuceneTestCase {

  public static final String FIELD = "field";
  public static final Analyzer ANALYZER = new StandardAnalyzer();

  public static Query parse(String query) {
    QueryParser parser = new QueryParser(FIELD, ANALYZER);
    try {
      return parser.parse(query);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static MonitorQuery mq(String id, String query, String... metadata) {
    Query q = parse(query);
    assert metadata.length % 2 == 0;
    Map<String, String> mm = new HashMap<>();
    for (int i = 0; i < metadata.length / 2; i += 2) {
      mm.put(metadata[i], metadata[i + 1]);
    }
    return new MonitorQuery(id, q, query, mm);
  }

  protected Monitor newMonitor() throws IOException {
    return newMonitor(new StandardAnalyzer());
  }

  protected Monitor newMonitor(Analyzer analyzer) throws IOException {
    // TODO: randomize presearcher
    return new Monitor(analyzer);
  }

  public static class ThrowOnRewriteQuery extends Query {

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      throw new IOException("Error rewriting");
    }

    @Override
    public String toString(String field) {
      return "ThrowOnRewriteQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }
}
