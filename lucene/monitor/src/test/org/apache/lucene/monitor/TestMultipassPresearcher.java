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

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
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

      MatchingQueries<QueryMatch> matches = monitor.match(buildDoc("field", "hello world and goodbye"),
          QueryMatch.SIMPLE_MATCHER);
      assertEquals(2, matches.getQueriesRun());
      assertNotNull(matches.matches("1"));
    }
  }

  public void testComplexBoolean() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", parse("field:(+foo +bar +(badger cormorant))")));

      MatchingQueries<QueryMatch> matches
          = monitor.match(buildDoc("field", "a badger walked into a bar"), QueryMatch.SIMPLE_MATCHER);
      assertEquals(0, matches.getMatchCount());
      assertEquals(0, matches.getQueriesRun());

      matches = monitor.match(buildDoc("field", "foo badger cormorant"), QueryMatch.SIMPLE_MATCHER);
      assertEquals(0, matches.getMatchCount());
      assertEquals(0, matches.getQueriesRun());

      matches = monitor.match(buildDoc("field", "bar badger foo"), QueryMatch.SIMPLE_MATCHER);
      assertEquals(1, matches.getMatchCount());
    }

  }

  public void testQueryBuilder() throws IOException {

    IndexWriterConfig iwc = new IndexWriterConfig(new KeywordAnalyzer());
    Presearcher presearcher = createPresearcher();

    Directory dir = new ByteBuffersDirectory();
    IndexWriter writer = new IndexWriter(dir, iwc);
    MonitorConfiguration config = new MonitorConfiguration(){
      @Override
      public IndexWriter buildIndexWriter() {
        return writer;
      }
    };
    try (Monitor monitor = new Monitor(ANALYZER, presearcher, config)) {

      monitor.register(new MonitorQuery("1", parse("f:test")));

      try (IndexReader reader = DirectoryReader.open(writer, false, false)) {

        MemoryIndex mindex = new MemoryIndex();
        mindex.addField("f", "this is a test document", WHITESPACE);
        LeafReader docsReader = (LeafReader) mindex.createSearcher().getIndexReader();

        QueryIndex.QueryTermFilter termFilter = new QueryIndex.QueryTermFilter(reader);

        BooleanQuery q = (BooleanQuery) presearcher.buildQuery(docsReader, termFilter);
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
