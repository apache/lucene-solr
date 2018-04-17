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

package org.apache.lucene.search.matchhighlight;

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;

public class TestMatchHighlighter extends LuceneTestCase {

  private MockAnalyzer indexAnalyzer;
  private Directory dir;

  @Before
  public void doBefore() {
    indexAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);//whitespace, punctuation, lowercase
    indexAnalyzer.setEnableChecks(false); // highlighters will not necessarily exhaust tokenstreams
    dir = newDirectory();
  }

  @After
  public void doAfter() throws IOException {
    dir.close();
  }

  public void testBasics() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", TextField.TYPE_STORED);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    body.setStringValue("Highlighting the first term. Hope it works.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    Query query = new TermQuery(new Term("body", "highlighting"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);

    MatchHighlighter highlighter = new MatchHighlighter(searcher, indexAnalyzer);
    TopHighlights highlights = highlighter.highlight(query, topDocs,
        () -> new PassageCollector(Collections.singleton("body"), 1, SentencePassageBuilder::new));

    assertEquals(2, highlights.docs.length);
    assertEquals("Just a test <b>highlighting</b> from postings. ", highlights.docs[0].fields.get("body"));
    assertEquals("<b>Highlighting</b> the first term. ", highlights.docs[1].fields.get("body"));

    ir.close();
  }

  // simple test highlighting last word.
  public void testHighlightLastWord() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", TextField.TYPE_STORED);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    Query query = new TermQuery(new Term("body", "test"));

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);

    MatchHighlighter highlighter = new MatchHighlighter(searcher, indexAnalyzer);
    TopHighlights highlights = highlighter.highlight(query, topDocs,
        () -> new PassageCollector(Collections.singleton("body"), 1, SentencePassageBuilder::new));
    assertEquals(1, highlights.docs.length);
    assertEquals("This is a <b>test</b>", highlights.docs[0].fields.get("body"));

    ir.close();
  }

  public void testMultiValuedField() throws Exception {

    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);
    Document doc = new Document();
    doc.add(new TextField("body", "This is the first sentence, and a fine sentence it is too", Field.Store.YES));
    doc.add(new TextField("body", "And this is the second sentence", Field.Store.YES));
    doc.add(new TextField("body", "And a third sentence too!", Field.Store.YES));
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    Query query = new TermQuery(new Term("body", "sentence"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);

    MatchHighlighter highlighter = new MatchHighlighter(searcher, indexAnalyzer);
    TopHighlights highlights = highlighter.highlight(query, topDocs,
        () -> new PassageCollector(Collections.singleton("body"), 3, SentencePassageBuilder::new));
    assertEquals(1, highlights.docs.length);
    String[] values = highlights.docs[0].fields.getValues("body");
    assertEquals(3, values.length);
    assertEquals("This is the first <b>sentence</b>, and a fine <b>sentence</b> it is too", values[0]);
    assertEquals("And this is the second <b>sentence</b>", values[1]);
    assertEquals("And a third <b>sentence</b> too!", values[2]);

    // again, this time with only one passage per field
    highlights = highlighter.highlight(query, topDocs,
        () -> new PassageCollector(Collections.singleton("body"), 1, SentencePassageBuilder::new));
    assertEquals(1, highlights.docs[0].fields.getValues("body").length);

    ir.close();
  }

}
