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
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.IOException;

public class TestSegmentTermDocs extends TestCase {
  private Document testDoc = new Document();
  private Directory dir = new RAMDirectory();

  public TestSegmentTermDocs(String s) {
    super(s);
  }

  protected void setUp() {
    DocHelper.setupDoc(testDoc);
    DocHelper.writeDoc(dir, testDoc);
  }


  protected void tearDown() {

  }

  public void test() {
    assertTrue(dir != null);
  }
  
  public void testTermDocs() {
    try {
      //After adding the document, we should be able to read it back in
      SegmentReader reader = new SegmentReader(new SegmentInfo("test", 1, dir));
      assertTrue(reader != null);
      SegmentTermDocs segTermDocs = new SegmentTermDocs(reader);
      assertTrue(segTermDocs != null);
      segTermDocs.seek(new Term(DocHelper.TEXT_FIELD_2_KEY, "field"));
      if (segTermDocs.next() == true)
      {
        int docId = segTermDocs.doc();
        assertTrue(docId == 0);
        int freq = segTermDocs.freq();
        assertTrue(freq == 3);  
      }
      reader.close();
    } catch (IOException e) {
      assertTrue(false);
    }
  }  
  
  public void testBadSeek() {
    try {
      //After adding the document, we should be able to read it back in
      SegmentReader reader = new SegmentReader(new SegmentInfo("test", 3, dir));
      assertTrue(reader != null);
      SegmentTermDocs segTermDocs = new SegmentTermDocs(reader);
      assertTrue(segTermDocs != null);
      segTermDocs.seek(new Term("textField2", "bad"));
      assertTrue(segTermDocs.next() == false);
      reader.close();
    } catch (IOException e) {
      assertTrue(false);
    }
    try {
      //After adding the document, we should be able to read it back in
      SegmentReader reader = new SegmentReader(new SegmentInfo("test", 3, dir));
      assertTrue(reader != null);
      SegmentTermDocs segTermDocs = new SegmentTermDocs(reader);
      assertTrue(segTermDocs != null);
      segTermDocs.seek(new Term("junk", "bad"));
      assertTrue(segTermDocs.next() == false);
      reader.close();
    } catch (IOException e) {
      assertTrue(false);
    }
  }    
}
