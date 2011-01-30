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

package org.apache.lucene.index;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;

public class TestIndexSorter extends LuceneTestCase {
  
  private static final int NUM_DOCS = 4;
  private String[] fieldNames = new String[] {
      "id",
      "url",
      "site",
      "content",
      "host",
      "anchor",
      "boost"
  };
  
  Directory inputDir = null;
  Directory outputDir = null;
  
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // create test index
    inputDir = new RAMDirectory();
    IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_31, new WhitespaceAnalyzer(Version.LUCENE_31));
    IndexWriter writer = new IndexWriter(inputDir, cfg);
    // create test documents
    for (int i = 0; i < NUM_DOCS; i++) {
      Document doc = new Document();
      for (int k = 0; k < fieldNames.length; k++) {
        Field f;
        Store s;
        Index ix;
        TermVector tv = TermVector.NO;
        String val = null;
        if (fieldNames[k].equals("id")) {
          s = Store.YES;
          ix = Index.NOT_ANALYZED;
          val = String.valueOf(i);
        } else if (fieldNames[k].equals("host")) {
          s = Store.YES;
          ix = Index.NOT_ANALYZED;
          val = "www.example" + i + ".com";
        } else if (fieldNames[k].equals("site")) {
          s = Store.NO;
          ix = Index.NOT_ANALYZED;
          val = "www.example" + i + ".com";
        } else if (fieldNames[k].equals("content")) {
          s = Store.NO;
          ix = Index.ANALYZED;
          tv = TermVector.YES;
          val = "This is the content of the " + i + "-th document.";
        } else if (fieldNames[k].equals("boost")) {
          s = Store.YES;
          ix = Index.NO;
          float boost = (float)i;
          val = String.valueOf(boost);
        } else {
          s = Store.YES;
          ix = Index.ANALYZED;
          if (fieldNames[k].equals("anchor")) {
            val = "anchors to " + i + "-th page.";
          } else if (fieldNames[k].equals("url")) {
            val = "http://www.example" + i + ".com/" + i + ".html";
          }
        }
        f = new Field(fieldNames[k], val, s, ix, tv);
        doc.add(f);
      }
      writer.addDocument(doc);
    }
    writer.optimize();
    writer.close();
    outputDir = new RAMDirectory();
  }
  
  public void testSorting() throws Exception {
    IndexSorter sorter = new IndexSorter();
    sorter.sort(inputDir, outputDir, "boost");
    
    // read back documents
    IndexReader reader = IndexReader.open(outputDir);
    assertEquals(reader.numDocs(), NUM_DOCS);
    for (int i = 0; i < reader.maxDoc(); i++) {
      Document doc = reader.document(i);
      Field f = doc.getField("content");
      assertNull(f);
      String boost = doc.get("boost");
      int origId = NUM_DOCS - i - 1;
      String cmp = String.valueOf((float)origId);
      assertEquals(cmp, boost);
      // check that vectors are in sync
      TermFreqVector tfv = reader.getTermFreqVector(i, "content");
      assertTrue(tfv.indexOf(origId + "-th") != -1);
    }
    reader.close();
  }

}
