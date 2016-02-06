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
package org.apache.lucene.analysis.uima;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Testcase for {@link UIMABaseAnalyzer}
 */
public class UIMABaseAnalyzerTest extends BaseTokenStreamTestCase {

  private UIMABaseAnalyzer analyzer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new UIMABaseAnalyzer("/uima/AggregateSentenceAE.xml", "org.apache.uima.TokenAnnotation", null);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    analyzer.close();
    super.tearDown();
  }

  @Test
  public void baseUIMAAnalyzerStreamTest() throws Exception {
    TokenStream ts = analyzer.tokenStream("text", "the big brown fox jumped on the wood");
    assertTokenStreamContents(ts, new String[]{"the", "big", "brown", "fox", "jumped", "on", "the", "wood"});
  }

  @Test
  public void baseUIMAAnalyzerIntegrationTest() throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    // add the first doc
    Document doc = new Document();
    String dummyTitle = "this is a dummy title ";
    doc.add(new TextField("title", dummyTitle, Field.Store.YES));
    String dummyContent = "there is some content written here";
    doc.add(new TextField("contents", dummyContent, Field.Store.YES));
    writer.addDocument(doc);
    writer.commit();

    // try the search over the first doc
    DirectoryReader directoryReader = DirectoryReader.open(dir);
    IndexSearcher indexSearcher = newSearcher(directoryReader);
    TopDocs result = indexSearcher.search(new MatchAllDocsQuery(), 1);
    assertTrue(result.totalHits > 0);
    Document d = indexSearcher.doc(result.scoreDocs[0].doc);
    assertNotNull(d);
    assertNotNull(d.getField("title"));
    assertEquals(dummyTitle, d.getField("title").stringValue());
    assertNotNull(d.getField("contents"));
    assertEquals(dummyContent, d.getField("contents").stringValue());

    // add a second doc
    doc = new Document();
    String dogmasTitle = "dogmas";
    doc.add(new TextField("title", dogmasTitle, Field.Store.YES));
    String dogmasContents = "white men can't jump";
    doc.add(new TextField("contents", dogmasContents, Field.Store.YES));
    writer.addDocument(doc);
    writer.commit();

    directoryReader.close();
    directoryReader = DirectoryReader.open(dir);
    indexSearcher = newSearcher(directoryReader);
    result = indexSearcher.search(new MatchAllDocsQuery(), 2);
    Document d1 = indexSearcher.doc(result.scoreDocs[1].doc);
    assertNotNull(d1);
    assertNotNull(d1.getField("title"));
    assertEquals(dogmasTitle, d1.getField("title").stringValue());
    assertNotNull(d1.getField("contents"));
    assertEquals(dogmasContents, d1.getField("contents").stringValue());

    // do a matchalldocs query to retrieve both docs
    result = indexSearcher.search(new MatchAllDocsQuery(), 2);
    assertEquals(2, result.totalHits);
    writer.close();
    indexSearcher.getIndexReader().close();
    dir.close();
  }

  @Test @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-3869")
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new UIMABaseAnalyzer("/uima/TestAggregateSentenceAE.xml", "org.apache.lucene.uima.ts.TokenAnnotation", null);
    checkRandomData(random(), analyzer, 100 * RANDOM_MULTIPLIER);
    analyzer.close();
  }

  @Test @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-3869")
  public void testRandomStringsWithConfigurationParameters() throws Exception {
    Map<String, Object> cp = new HashMap<>();
    cp.put("line-end", "\r");
    Analyzer analyzer = new UIMABaseAnalyzer("/uima/TestWSTokenizerAE.xml", "org.apache.lucene.uima.ts.TokenAnnotation", cp);
    checkRandomData(random(), analyzer, 100 * RANDOM_MULTIPLIER);
    analyzer.close();
  }

}
