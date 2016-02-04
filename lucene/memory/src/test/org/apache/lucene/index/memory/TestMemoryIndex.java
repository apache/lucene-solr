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
package org.apache.lucene.index.memory;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockPayloadAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.internal.matchers.StringContains.containsString;

public class TestMemoryIndex extends LuceneTestCase {

  private MockAnalyzer analyzer;

  @Before
  public void setup() {
    analyzer = new MockAnalyzer(random());
    analyzer.setEnableChecks(false);    // MemoryIndex can close a TokenStream on init error
  }

  @Test
  public void testFreezeAPI() {

    MemoryIndex mi = new MemoryIndex();
    mi.addField("f1", "some text", analyzer);

    assertThat(mi.search(new MatchAllDocsQuery()), not(is(0.0f)));
    assertThat(mi.search(new TermQuery(new Term("f1", "some"))), not(is(0.0f)));

    // check we can add a new field after searching
    mi.addField("f2", "some more text", analyzer);
    assertThat(mi.search(new TermQuery(new Term("f2", "some"))), not(is(0.0f)));

    // freeze!
    mi.freeze();

    try {
      mi.addField("f3", "and yet more", analyzer);
      fail("Expected an IllegalArgumentException when adding a field after calling freeze()");
    }
    catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("frozen"));
    }

    try {
      mi.setSimilarity(new BM25Similarity(1, 1));
      fail("Expected an IllegalArgumentException when setting the Similarity after calling freeze()");
    }
    catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("frozen"));
    }

    assertThat(mi.search(new TermQuery(new Term("f1", "some"))), not(is(0.0f)));

    mi.reset();
    mi.addField("f1", "wibble", analyzer);
    assertThat(mi.search(new TermQuery(new Term("f1", "some"))), is(0.0f));
    assertThat(mi.search(new TermQuery(new Term("f1", "wibble"))), not(is(0.0f)));

    // check we can set the Similarity again
    mi.setSimilarity(new DefaultSimilarity());

  }

  public void testSeekByTermOrd() throws IOException {
    MemoryIndex mi = new MemoryIndex();
    mi.addField("field", "some terms be here", analyzer);
    IndexSearcher searcher = mi.createSearcher();
    LeafReader reader = (LeafReader) searcher.getIndexReader();
    TermsEnum terms = reader.fields().terms("field").iterator();
    terms.seekExact(0);
    assertEquals("be", terms.term().utf8ToString());
    TestUtil.checkReader(reader);
  }
  
  public void testReaderConsistency() throws IOException {
    Analyzer analyzer = new MockPayloadAnalyzer();
    
    // defaults
    MemoryIndex mi = new MemoryIndex();
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());
    
    // all combinations of offsets/payloads options
    mi = new MemoryIndex(true, true);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());
    
    mi = new MemoryIndex(true, false);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());
    
    mi = new MemoryIndex(false, true);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());
    
    mi = new MemoryIndex(false, false);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());
    
    analyzer.close();
  }

  @Test
  public void testSimilarities() throws IOException {

    MemoryIndex mi = new MemoryIndex();
    mi.addField("f1", "a long text field that contains many many terms", analyzer);

    IndexSearcher searcher = mi.createSearcher();
    LeafReader reader = (LeafReader) searcher.getIndexReader();
    float n1 = reader.getNormValues("f1").get(0);

    // Norms are re-computed when we change the Similarity
    mi.setSimilarity(new DefaultSimilarity() {
      @Override
      public float lengthNorm(FieldInvertState state) {
        return 74;
      }
    });
    float n2 = reader.getNormValues("f1").get(0);

    assertTrue(n1 != n2);
    TestUtil.checkReader(reader);
  }

  @Test
  public void testBuildFromDocument() {

    Document doc = new Document();
    doc.add(new TextField("field1", "some text", Field.Store.NO));
    doc.add(new TextField("field1", "some more text", Field.Store.NO));
    doc.add(new StringField("field2", "untokenized text", Field.Store.NO));

    analyzer.setPositionIncrementGap(100);

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);

    assertThat(mi.search(new TermQuery(new Term("field1", "text"))), not(0.0f));
    assertThat(mi.search(new TermQuery(new Term("field2", "text"))), is(0.0f));
    assertThat(mi.search(new TermQuery(new Term("field2", "untokenized text"))), not(0.0f));

    assertThat(mi.search(new PhraseQuery("field1", "some", "more", "text")), not(0.0f));
    assertThat(mi.search(new PhraseQuery("field1", "some", "text")), not(0.0f));
    assertThat(mi.search(new PhraseQuery("field1", "text", "some")), is(0.0f));

  }


}
