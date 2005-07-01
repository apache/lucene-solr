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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.search.Similarity;

import java.io.IOException;

public class TestFieldsReader extends TestCase {
  private RAMDirectory dir = new RAMDirectory();
  private Document testDoc = new Document();
  private FieldInfos fieldInfos = null;

  public TestFieldsReader(String s) {
    super(s);
  }

  protected void setUp() throws IOException {
    fieldInfos = new FieldInfos();
    DocHelper.setupDoc(testDoc);
    fieldInfos.add(testDoc);
    DocumentWriter writer = new DocumentWriter(dir, new WhitespaceAnalyzer(),
            Similarity.getDefault(), 50);
    assertTrue(writer != null);
    writer.addDocument("test", testDoc);
  }

  public void test() throws IOException {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    FieldsReader reader = new FieldsReader(dir, "test", fieldInfos);
    assertTrue(reader != null);
    assertTrue(reader.size() == 1);
    Document doc = reader.doc(0);
    assertTrue(doc != null);
    assertTrue(doc.getField("textField1") != null);
    Field field = doc.getField("textField2");
    assertTrue(field != null);
    assertTrue(field.isTermVectorStored() == true);
    reader.close();
  }
}
