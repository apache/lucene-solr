package org.apache.lucene.analysis.core;

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

import java.io.StringReader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util._TestUtil;

public class TestKeywordAnalyzer extends BaseTokenStreamTestCase {
  
  private Directory directory;
  private IndexSearcher searcher;
  private IndexReader reader;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new SimpleAnalyzer(TEST_VERSION_CURRENT)));

    Document doc = new Document();
    doc.add(new StringField("partnum", "Q36", Field.Store.YES));
    doc.add(new TextField("description", "Illidium Space Modulator", Field.Store.YES));
    writer.addDocument(doc);

    writer.close();

    reader = DirectoryReader.open(directory);
    searcher = newSearcher(reader);
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  /*
  public void testPerFieldAnalyzer() throws Exception {
    PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new SimpleAnalyzer(TEST_VERSION_CURRENT));
    analyzer.addAnalyzer("partnum", new KeywordAnalyzer());

    QueryParser queryParser = new QueryParser(TEST_VERSION_CURRENT, "description", analyzer);
    Query query = queryParser.parse("partnum:Q36 AND SPACE");

    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("Q36 kept as-is",
              "+partnum:Q36 +space", query.toString("description"));
    assertEquals("doc found!", 1, hits.length);
  }
  */

  public void testMutipleDocument() throws Exception {
    RAMDirectory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new KeywordAnalyzer()));
    Document doc = new Document();
    doc.add(new TextField("partnum", "Q36", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("partnum", "Q37", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    DocsEnum td = _TestUtil.docs(random(),
                                 reader,
                                 "partnum",
                                 new BytesRef("Q36"),
                                 MultiFields.getLiveDocs(reader),
                                 null,
                                 0);
    assertTrue(td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    td = _TestUtil.docs(random(),
                        reader,
                        "partnum",
                        new BytesRef("Q37"),
                        MultiFields.getLiveDocs(reader),
                        null,
                        0);
    assertTrue(td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
  }

  // LUCENE-1441
  public void testOffsets() throws Exception {
    TokenStream stream = new KeywordAnalyzer().tokenStream("field", new StringReader("abcd"));
    OffsetAttribute offsetAtt = stream.addAttribute(OffsetAttribute.class);
    stream.reset();
    assertTrue(stream.incrementToken());
    assertEquals(0, offsetAtt.startOffset());
    assertEquals(4, offsetAtt.endOffset());
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), new KeywordAnalyzer(), 1000*RANDOM_MULTIPLIER);
  }
}
