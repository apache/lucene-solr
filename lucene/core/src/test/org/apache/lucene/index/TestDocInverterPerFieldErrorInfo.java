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
package org.apache.lucene.index;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Test adding to the info stream when there's an exception thrown during field analysis.
 */
public class TestDocInverterPerFieldErrorInfo extends LuceneTestCase {
  private static final FieldType storedTextType = new FieldType(TextField.TYPE_NOT_STORED);

  private static class BadNews extends RuntimeException {
    private BadNews(String message) {
      super(message);
    }
  }

  private static class ThrowingAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new MockTokenizer();
      if (fieldName.equals("distinctiveFieldName")) {
        TokenFilter tosser = new TokenFilter(tokenizer) {
          @Override
          public boolean incrementToken() throws IOException {
            throw new BadNews("Something is icky.");
          }
        };
        return new TokenStreamComponents(tokenizer, tosser);
      } else {
        return new TokenStreamComponents(tokenizer);
      }
    }
  }

  @Test
  public void testInfoStreamGetsFieldName() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer;
    IndexWriterConfig c = new IndexWriterConfig(new ThrowingAnalyzer());
    final ByteArrayOutputStream infoBytes = new ByteArrayOutputStream();
    PrintStream infoPrintStream = new PrintStream(infoBytes, true, IOUtils.UTF_8);
    PrintStreamInfoStream printStreamInfoStream = new PrintStreamInfoStream(infoPrintStream);
    c.setInfoStream(printStreamInfoStream);
    writer = new IndexWriter(dir, c);
    Document doc = new Document();
    doc.add(newField("distinctiveFieldName", "aaa ", storedTextType));
    expectThrows(BadNews.class, () -> {
      writer.addDocument(doc);
    });
    infoPrintStream.flush();
    String infoStream = new String(infoBytes.toByteArray(), IOUtils.UTF_8);
    assertTrue(infoStream.contains("distinctiveFieldName"));

    writer.close();
    dir.close();
  }

  @Test
  public void testNoExtraNoise() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer;
    IndexWriterConfig c = new IndexWriterConfig(new ThrowingAnalyzer());
    final ByteArrayOutputStream infoBytes = new ByteArrayOutputStream();
    PrintStream infoPrintStream = new PrintStream(infoBytes, true, IOUtils.UTF_8);
    PrintStreamInfoStream printStreamInfoStream = new PrintStreamInfoStream(infoPrintStream);
    c.setInfoStream(printStreamInfoStream);
    writer = new IndexWriter(dir, c);
    Document doc = new Document();
    doc.add(newField("boringFieldName", "aaa ", storedTextType));
    try {
      writer.addDocument(doc);
    } catch(BadNews badNews) {
      fail("Unwanted exception");
    }
    infoPrintStream.flush();
    String infoStream = new String(infoBytes.toByteArray(), IOUtils.UTF_8);
    assertFalse(infoStream.contains("boringFieldName"));

    writer.close();
    dir.close();
  }

}
