package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.IOException;

public class TestDocumentWriter extends TestCase {
  private RAMDirectory dir = new RAMDirectory();
  private Document testDoc = new Document();


  public TestDocumentWriter(String s) {
    super(s);
  }

  protected void setUp() {
    DocHelper.setupDoc(testDoc);
  }

  protected void tearDown() {

  }

  public void test() {
    assertTrue(dir != null);

  }

  public void testAddDocument() {
    Analyzer analyzer = new WhitespaceAnalyzer();
    Similarity similarity = Similarity.getDefault();
    DocumentWriter writer = new DocumentWriter(dir, analyzer, similarity, 50);
    assertTrue(writer != null);
    try {
      writer.addDocument("test", testDoc);
      //After adding the document, we should be able to read it back in
      SegmentReader reader = new SegmentReader(new SegmentInfo("test", 1, dir));
      assertTrue(reader != null);
      Document doc = reader.document(0);
      assertTrue(doc != null);
      
      //System.out.println("Document: " + doc);
      Field [] fields = doc.getFields("textField2");
      assertTrue(fields != null && fields.length == 1);
      assertTrue(fields[0].stringValue().equals(DocHelper.FIELD_2_TEXT));
      assertTrue(fields[0].isTermVectorStored() == true);
      
      fields = doc.getFields("textField1");
      assertTrue(fields != null && fields.length == 1);
      assertTrue(fields[0].stringValue().equals(DocHelper.FIELD_1_TEXT));
      assertTrue(fields[0].isTermVectorStored() == false);
      
      fields = doc.getFields("keyField");
      assertTrue(fields != null && fields.length == 1);
      assertTrue(fields[0].stringValue().equals(DocHelper.KEYWORD_TEXT));
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
}
