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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

public class TestSpanFirstQuery extends LuceneTestCase {
  public void testStartPositions() throws Exception {
    Directory dir = newDirectory();
    
    // mimic StopAnalyzer
    CharacterRunAutomaton stopSet = new CharacterRunAutomaton(new RegExp("the|a|of").toAutomaton());
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopSet, true);
    
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, analyzer);
    Document doc = new Document();
    doc.add(newTextField("field", "the quick brown fox", Field.Store.NO));
    writer.addDocument(doc);
    Document doc2 = new Document();
    doc2.add(newTextField("field", "quick brown fox", Field.Store.NO));
    writer.addDocument(doc2);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    
    // user queries on "starts-with quick"
    SpanQuery sfq = new SpanFirstQuery(new SpanTermQuery(new Term("field", "quick")), 1);
    assertEquals(1, searcher.search(sfq, 10).totalHits);
    
    // user queries on "starts-with the quick"
    SpanQuery include = new SpanFirstQuery(new SpanTermQuery(new Term("field", "quick")), 2);
    sfq = new SpanNotQuery(include, sfq);
    assertEquals(1, searcher.search(sfq, 10).totalHits);
    
    writer.close();
    reader.close();
    dir.close();
  }
}
