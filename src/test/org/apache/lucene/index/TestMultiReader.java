package org.apache.lucene.index;

/**
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

import junit.framework.TestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

public class TestMultiReader extends TestCase {
  private Directory dir = new RAMDirectory();
  private Document doc1 = new Document();
  private Document doc2 = new Document();
  private SegmentReader reader1;
  private SegmentReader reader2;
  private SegmentReader [] readers = new SegmentReader[2];
  private SegmentInfos sis = new SegmentInfos();
  
  public TestMultiReader(String s) {
    super(s);
  }

  protected void setUp() throws IOException {
    DocHelper.setupDoc(doc1);
    DocHelper.setupDoc(doc2);
    DocHelper.writeDoc(dir, "seg-1", doc1);
    DocHelper.writeDoc(dir, "seg-2", doc2);
    sis.write(dir);
    reader1 = SegmentReader.get(new SegmentInfo("seg-1", 1, dir));
    reader2 = SegmentReader.get(new SegmentInfo("seg-2", 1, dir));
    readers[0] = reader1;
    readers[1] = reader2;
  }
  
  public void test() {
    assertTrue(dir != null);
    assertTrue(reader1 != null);
    assertTrue(reader2 != null);
    assertTrue(sis != null);
  }    

  public void testDocument() throws IOException {
    sis.read(dir);
    MultiReader reader = new MultiReader(dir, sis, false, readers);
    assertTrue(reader != null);
    Document newDoc1 = reader.document(0);
    assertTrue(newDoc1 != null);
    assertTrue(DocHelper.numFields(newDoc1) == DocHelper.numFields(doc1) - DocHelper.unstored.size());
    Document newDoc2 = reader.document(1);
    assertTrue(newDoc2 != null);
    assertTrue(DocHelper.numFields(newDoc2) == DocHelper.numFields(doc2) - DocHelper.unstored.size());
    TermFreqVector vector = reader.getTermFreqVector(0, DocHelper.TEXT_FIELD_2_KEY);
    assertTrue(vector != null);
    TestSegmentReader.checkNorms(reader);
  }

  public void testUndeleteAll() throws IOException {
    sis.read(dir);
    MultiReader reader = new MultiReader(dir, sis, false, readers);
    assertTrue(reader != null);
    assertEquals( 2, reader.numDocs() );
    reader.deleteDocument(0);
    assertEquals( 1, reader.numDocs() );
    reader.undeleteAll();
    assertEquals( 2, reader.numDocs() );

    // Ensure undeleteAll survives commit/close/reopen:
    reader.commit();
    reader.close();
    sis.read(dir);
    reader = new MultiReader(dir, sis, false, readers);
    assertEquals( 2, reader.numDocs() );

    reader.deleteDocument(0);
    assertEquals( 1, reader.numDocs() );
    reader.commit();
    reader.close();
    sis.read(dir);
    reader = new MultiReader(dir, sis, false, readers);
    assertEquals( 1, reader.numDocs() );
  }
        
  
  public void testTermVectors() {
    MultiReader reader = new MultiReader(dir, sis, false, readers);
    assertTrue(reader != null);
  }    
}
