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

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.InputDocument;
import org.apache.lucene.luwak.Matches;
import org.apache.lucene.luwak.Monitor;
import org.apache.lucene.luwak.MonitorQuery;
import org.apache.lucene.luwak.Presearcher;
import org.apache.lucene.luwak.QueryIndexConfiguration;
import org.apache.lucene.luwak.QueryMatch;
import org.apache.lucene.luwak.QueryTermFilter;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

public class TestMultipassPresearcher extends PresearcherTestBase {

  @Override
  protected Presearcher createPresearcher() {
    return new MultipassTermFilteredPresearcher(4);
  }

  public void testSimpleBoolean() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.register(
          new MonitorQuery("1", parse("field:\"hello world\"")),
          new MonitorQuery("2", parse("field:world")),
          new MonitorQuery("3", parse("field:\"hello there world\"")),
          new MonitorQuery("4", parse("field:\"this and that\"")));

      Matches<QueryMatch> matches = monitor.match(buildDoc("doc1", "field", "hello world and goodbye"),
          SimpleMatcher.FACTORY);
      assertEquals(2, matches.getQueriesRun());
      assertNotNull(matches.matches("1", "doc1"));
    }
  }

  public void testComplexBoolean() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", parse("field:(+foo +bar +(badger cormorant))")));

      Matches<QueryMatch> matches
          = monitor.match(buildDoc("doc1", "field", "a badger walked into a bar"), SimpleMatcher.FACTORY);
      assertEquals(0, matches.getMatchCount("doc1"));
      assertEquals(0, matches.getQueriesRun());

      matches = monitor.match(buildDoc("doc2", "field", "foo badger cormorant"), SimpleMatcher.FACTORY);
      assertEquals(0, matches.getMatchCount("doc2"));
      assertEquals(0, matches.getQueriesRun());

      matches = monitor.match(buildDoc("doc3", "field", "bar badger foo"), SimpleMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc3"));
    }

  }

  public void testQueryBuilder() throws IOException {

    IndexWriterConfig iwc = new IndexWriterConfig(new KeywordAnalyzer());
    Presearcher presearcher = createPresearcher();

    Directory dir = new ByteBuffersDirectory();
    IndexWriter writer = new IndexWriter(dir, iwc);
    QueryIndexConfiguration config = new QueryIndexConfiguration(){
      @Override
      public IndexWriter buildIndexWriter() {
        return writer;
      }
    };
    try (Monitor monitor = new Monitor(presearcher, config)) {

      monitor.register(new MonitorQuery("1", parse("f:test")));

      try (IndexReader reader = DirectoryReader.open(writer, false, false)) {

        InputDocument doc = InputDocument.builder("doc1")
            .addField("f", "this is a test document", WHITESPACE).build();
        DocumentBatch docs = DocumentBatch.of(doc);

        BooleanQuery q = (BooleanQuery) presearcher.buildQuery(docs.getIndexReader(), new QueryTermFilter(reader));
        BooleanQuery expected = new BooleanQuery.Builder()
            .add(should(new BooleanQuery.Builder()
                .add(must(new BooleanQuery.Builder().add(should(new TermInSetQuery("f_0", new BytesRef("test")))).build()))
                .add(must(new BooleanQuery.Builder().add(should(new TermInSetQuery("f_1", new BytesRef("test")))).build()))
                .add(must(new BooleanQuery.Builder().add(should(new TermInSetQuery("f_2", new BytesRef("test")))).build()))
                .add(must(new BooleanQuery.Builder().add(should(new TermInSetQuery("f_3", new BytesRef("test")))).build()))
                .build()))
            .add(should(new TermQuery(new Term("__anytokenfield", "__ANYTOKEN__"))))
            .build();

        assertEquals(expected, q);
      }

    }

  }

}
