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
package org.apache.lucene.sandbox.queries.regex;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestSpanRegexQuery extends LuceneTestCase {
  
  Directory indexStoreA;
  Directory indexStoreB;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    indexStoreA = newDirectory();
    indexStoreB = newDirectory();
  }
  
  @Override
  public void tearDown() throws Exception {
    indexStoreA.close();
    indexStoreB.close();
    super.tearDown();
  }
  
  public void testSpanRegex() throws Exception {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(analyzer));
    Document doc = new Document();
    // doc.add(newField("field", "the quick brown fox jumps over the lazy dog",
    // Field.Store.NO, Field.Index.ANALYZED));
    // writer.addDocument(doc);
    // doc = new Document();
    doc.add(newTextField("field", "auto update", Field.Store.NO));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newTextField("field", "first auto update", Field.Store.NO));
    writer.addDocument(doc);
    writer.forceMerge(1);
    writer.close();

    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = newSearcher(reader);
    SpanQuery srq = new SpanMultiTermQueryWrapper<>(new RegexQuery(new Term("field", "aut.*")));
    SpanFirstQuery sfq = new SpanFirstQuery(srq, 1);
    // SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {srq, stq}, 6,
    // true);
    int numHits = searcher.search(sfq, 1000).totalHits;
    assertEquals(1, numHits);
    IOUtils.close(reader, directory, analyzer);
  }
}
