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
package org.apache.lucene.analysis.core;


import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;

public class TestKeywordAnalyzer extends BaseTokenStreamTestCase {
  
  private Directory directory;
  private IndexReader reader;
  private Analyzer analyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    analyzer = new SimpleAnalyzer();
    IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(analyzer));

    Document doc = new Document();
    doc.add(new StringField("partnum", "Q36", Field.Store.YES));
    doc.add(new TextField("description", "Illidium Space Modulator", Field.Store.YES));
    writer.addDocument(doc);

    writer.close();

    reader = DirectoryReader.open(directory);
  }
  
  @Override
  public void tearDown() throws Exception {
    IOUtils.close(analyzer, reader, directory);
    super.tearDown();
  }

  /*
  public void testPerFieldAnalyzer() throws Exception {
    PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new SimpleAnalyzer());
    analyzer.addAnalyzer("partnum", new KeywordAnalyzer());

    QueryParser queryParser = new QueryParser("description", analyzer);
    Query query = queryParser.parse("partnum:Q36 AND SPACE");

    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("Q36 kept as-is",
              "+partnum:Q36 +space", query.toString("description"));
    assertEquals("doc found!", 1, hits.length);
  }
  */

  public void testMutipleDocument() throws Exception {
    RAMDirectory dir = new RAMDirectory();
    Analyzer analyzer = new KeywordAnalyzer();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    Document doc = new Document();
    doc.add(new TextField("partnum", "Q36", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("partnum", "Q37", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    PostingsEnum td = TestUtil.docs(random(),
        reader,
        "partnum",
        new BytesRef("Q36"),
        null,
        0);
    assertTrue(td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    td = TestUtil.docs(random(),
        reader,
        "partnum",
        new BytesRef("Q37"),
        null,
        0);
    assertTrue(td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    analyzer.close();
  }

  // LUCENE-1441
  public void testOffsets() throws Exception {
    try (Analyzer analyzer = new KeywordAnalyzer();
         TokenStream stream = analyzer.tokenStream("field", new StringReader("abcd"))) {
      OffsetAttribute offsetAtt = stream.addAttribute(OffsetAttribute.class);
      stream.reset();
      assertTrue(stream.incrementToken());
      assertEquals(0, offsetAtt.startOffset());
      assertEquals(4, offsetAtt.endOffset());
      assertFalse(stream.incrementToken());
      stream.end();
    }
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new KeywordAnalyzer();
    checkRandomData(random(), analyzer, 1000*RANDOM_MULTIPLIER);
    analyzer.close();
  }
}
