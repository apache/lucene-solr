package org.apache.lucene.search.vectorhighlight;
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
import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;


public class FastVectorHighlighterTest extends LuceneTestCase {
  
  public void testSimpleHighlightTest() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    Field field = new Field("field", "This is a test where foo is highlighed and should be highlighted", type);
    
    doc.add(field);
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    
    IndexReader reader = DirectoryReader.open(writer, true);
    int docId = 0;
    FieldQuery fieldQuery  = highlighter.getFieldQuery( new TermQuery(new Term("field", "foo")), reader );
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 54, 1);
    // highlighted results are centered 
    assertEquals("This is a test where <b>foo</b> is highlighed and should be highlighted", bestFragments[0]);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 52, 1);
    assertEquals("This is a test where <b>foo</b> is highlighed and should be", bestFragments[0]);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 30, 1);
    assertEquals("a test where <b>foo</b> is highlighed", bestFragments[0]);
    reader.close();
    writer.close();
    dir.close();
  }
  
  public void testCommonTermsQueryHighlightTest() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT,  new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true)));
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    String[] texts = {
        "Hello this is a piece of text that is very long and contains too much preamble and the meat is really here which says kennedy has been shot",
        "This piece of text refers to Kennedy at the beginning then has a longer piece of text that is very long in the middle and finally ends with another reference to Kennedy",
        "JFK has been shot", "John Kennedy has been shot",
        "This text has a typo in referring to Keneddy",
        "wordx wordy wordz wordx wordy wordx worda wordb wordy wordc", "y z x y z a b", "lets is a the lets is a the lets is a the lets" };
    for (int i = 0; i < texts.length; i++) {
      Document doc = new Document();
      Field field = new Field("field", texts[i], type);
      doc.add(field);
      writer.addDocument(doc);
    }
    CommonTermsQuery query = new CommonTermsQuery(Occur.MUST, Occur.SHOULD, 2);
    query.add(new Term("field", "text"));
    query.add(new Term("field", "long"));
    query.add(new Term("field", "very"));
   
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    IndexReader reader = DirectoryReader.open(writer, true);
    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs hits = searcher.search(query, 10);
    assertEquals(2, hits.totalHits);
    FieldQuery fieldQuery  = highlighter.getFieldQuery(query, reader);
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, hits.scoreDocs[0].doc, "field", 1000, 1);
    assertEquals("This piece of <b>text</b> refers to Kennedy at the beginning then has a longer piece of <b>text</b> that is <b>very</b> <b>long</b> in the middle and finally ends with another reference to Kennedy", bestFragments[0]);

    fieldQuery  = highlighter.getFieldQuery(query, reader);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, hits.scoreDocs[1].doc, "field", 1000, 1);
    assertEquals("Hello this is a piece of <b>text</b> that is <b>very</b> <b>long</b> and contains too much preamble and the meat is really here which says kennedy has been shot", bestFragments[0]);

    reader.close();
    writer.close();
    dir.close();
  }
}
