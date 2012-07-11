package org.apache.lucene.analysis.query;
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

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;

public class QueryAutoStopWordAnalyzerTest extends BaseTokenStreamTestCase {
  String variedFieldValues[] = {"the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "boring", "dog"};
  String repetitiveFieldValues[] = {"boring", "boring", "vaguelyboring"};
  RAMDirectory dir;
  Analyzer appAnalyzer;
  IndexReader reader;
  QueryAutoStopWordAnalyzer protectedAnalyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = new RAMDirectory();
    appAnalyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, appAnalyzer));
    int numDocs = 200;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      String variedFieldValue = variedFieldValues[i % variedFieldValues.length];
      String repetitiveFieldValue = repetitiveFieldValues[i % repetitiveFieldValues.length];
      doc.add(new TextField("variedField", variedFieldValue, Field.Store.YES));
      doc.add(new TextField("repetitiveField", repetitiveFieldValue, Field.Store.YES));
      writer.addDocument(doc);
    }
    writer.close();
    reader = DirectoryReader.open(dir);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    super.tearDown();
  }

  public void testNoStopwords() throws Exception {
    // Note: an empty list of fields passed in
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Collections.<String>emptyList(), 1);
    TokenStream protectedTokenStream = protectedAnalyzer.tokenStream("variedField", new StringReader("quick"));
    assertTokenStreamContents(protectedTokenStream, new String[]{"quick"});

    protectedTokenStream = protectedAnalyzer.tokenStream("repetitiveField", new StringReader("boring"));
    assertTokenStreamContents(protectedTokenStream, new String[]{"boring"});
  }

  public void testDefaultStopwordsAllFields() throws Exception {
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader);
    TokenStream protectedTokenStream = protectedAnalyzer.tokenStream("repetitiveField", new StringReader("boring"));
    assertTokenStreamContents(protectedTokenStream, new String[0]); // Default stop word filtering will remove boring
  }

  public void testStopwordsAllFieldsMaxPercentDocs() throws Exception {
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, 1f / 2f);

    TokenStream protectedTokenStream = protectedAnalyzer.tokenStream("repetitiveField", new StringReader("boring"));
    // A filter on terms in > one half of docs remove boring
    assertTokenStreamContents(protectedTokenStream, new String[0]);

    protectedTokenStream = protectedAnalyzer.tokenStream("repetitiveField", new StringReader("vaguelyboring"));
     // A filter on terms in > half of docs should not remove vaguelyBoring
    assertTokenStreamContents(protectedTokenStream, new String[]{"vaguelyboring"});

    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, 1f / 4f);
    protectedTokenStream = protectedAnalyzer.tokenStream("repetitiveField", new StringReader("vaguelyboring"));
     // A filter on terms in > quarter of docs should remove vaguelyBoring
    assertTokenStreamContents(protectedTokenStream, new String[0]);
  }

  public void testStopwordsPerFieldMaxPercentDocs() throws Exception {
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Arrays.asList("variedField"), 1f / 2f);
    TokenStream protectedTokenStream = protectedAnalyzer.tokenStream("repetitiveField", new StringReader("boring"));
    // A filter on one Field should not affect queries on another
    assertTokenStreamContents(protectedTokenStream, new String[]{"boring"});

    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Arrays.asList("variedField", "repetitiveField"), 1f / 2f);
    protectedTokenStream = protectedAnalyzer.tokenStream("repetitiveField", new StringReader("boring"));
    // A filter on the right Field should affect queries on it
    assertTokenStreamContents(protectedTokenStream, new String[0]);
  }

  public void testStopwordsPerFieldMaxDocFreq() throws Exception {
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Arrays.asList("repetitiveField"), 10);
    int numStopWords = protectedAnalyzer.getStopWords("repetitiveField").length;
    assertTrue("Should have identified stop words", numStopWords > 0);

    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Arrays.asList("repetitiveField", "variedField"), 10);
    int numNewStopWords = protectedAnalyzer.getStopWords("repetitiveField").length + protectedAnalyzer.getStopWords("variedField").length;
    assertTrue("Should have identified more stop words", numNewStopWords > numStopWords);
  }

  public void testNoFieldNamePollution() throws Exception {
    protectedAnalyzer = new QueryAutoStopWordAnalyzer(TEST_VERSION_CURRENT, appAnalyzer, reader, Arrays.asList("repetitiveField"), 10);

    TokenStream protectedTokenStream = protectedAnalyzer.tokenStream("repetitiveField", new StringReader("boring"));
    // Check filter set up OK
    assertTokenStreamContents(protectedTokenStream, new String[0]);

    protectedTokenStream = protectedAnalyzer.tokenStream("variedField", new StringReader("boring"));
    // Filter should not prevent stopwords in one field being used in another
    assertTokenStreamContents(protectedTokenStream, new String[]{"boring"});
  }
  
  public void testTokenStream() throws Exception {
    QueryAutoStopWordAnalyzer a = new QueryAutoStopWordAnalyzer(
        TEST_VERSION_CURRENT,
        new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false), reader, 10);
    TokenStream ts = a.tokenStream("repetitiveField", new StringReader("this boring"));
    assertTokenStreamContents(ts, new String[] { "this" });
  }
}
