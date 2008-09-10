package org.apache.lucene.search.regex;

/**
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

import junit.framework.TestCase;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;

public class TestSpanRegexQuery extends TestCase {
  public void testSpanRegex() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new SimpleAnalyzer(), true);
    Document doc = new Document();
//    doc.add(new Field("field", "the quick brown fox jumps over the lazy dog", Field.Store.NO, Field.Index.ANALYZED));
//    writer.addDocument(doc);
//    doc = new Document();
    doc.add(new Field("field", "auto update", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new Field("field", "first auto update", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.optimize();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(directory);
    SpanRegexQuery srq = new SpanRegexQuery(new Term("field", "aut.*"));
    SpanFirstQuery sfq = new SpanFirstQuery(srq, 1);
//    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {srq, stq}, 6, true);
    Hits hits = searcher.search(sfq);
    assertEquals(1, hits.length());
  }
}
