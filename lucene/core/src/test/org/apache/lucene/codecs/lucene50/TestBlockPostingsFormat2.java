package org.apache.lucene.codecs.lucene50;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document2;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** 
 * Tests special cases of BlockPostingsFormat 
 */

public class TestBlockPostingsFormat2 extends LuceneTestCase {
  Directory dir;
  RandomIndexWriter iw;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newFSDirectory(createTempDir("testDFBlockSize"));
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new Lucene50PostingsFormat()));
    iw = new RandomIndexWriter(random(), dir, iwc);
    iw.setDoRandomForceMerge(false); // we will ourselves
  }
  
  @Override
  public void tearDown() throws Exception {
    iw.close();
    TestUtil.checkIndex(dir); // for some extra coverage, checkIndex before we forceMerge
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new Lucene50PostingsFormat()));
    iwc.setOpenMode(OpenMode.APPEND);
    IndexWriter iw = new IndexWriter(dir, iwc);
    iw.forceMerge(1);
    iw.close();
    dir.close(); // just force a checkindex for now
    super.tearDown();
  }
  
  private Document2 newDocument(String contents) {
    Document2 doc = iw.newDocument();
    FieldTypes fieldTypes = iw.getFieldTypes();
    for (IndexOptions option : IndexOptions.values()) {
      if (option == IndexOptions.NONE) {
        continue;
      }
      String fieldName = option.toString();
      // turn on tvs for a cross-check, since we rely upon checkindex in this test (for now)
      fieldTypes.disableHighlighting(fieldName);
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorOffsets(fieldName);
      fieldTypes.enableTermVectorPositions(fieldName);
      fieldTypes.enableTermVectorPayloads(fieldName);
      fieldTypes.setIndexOptions(fieldName, option);
      doc.addLargeText(fieldName, contents.replaceAll("name", fieldName));
    }
    return doc;
  }

  /** tests terms with df = blocksize */
  public void testDFBlockSize() throws Exception {
    for (int i = 0; i < Lucene50PostingsFormat.BLOCK_SIZE; i++) {
      iw.addDocument(newDocument("name name_2"));
    }
  }

  /** tests terms with df % blocksize = 0 */
  public void testDFBlockSizeMultiple() throws Exception {
    for (int i = 0; i < Lucene50PostingsFormat.BLOCK_SIZE * 16; i++) {
      iw.addDocument(newDocument("name name_2"));
    }
  }
  
  /** tests terms with ttf = blocksize */
  public void testTTFBlockSize() throws Exception {
    for (int i = 0; i < Lucene50PostingsFormat.BLOCK_SIZE/2; i++) {
      iw.addDocument(newDocument("name name name_2 name_2"));
    }
  }
  
  /** tests terms with ttf % blocksize = 0 */
  public void testTTFBlockSizeMultiple() throws Exception {
    String proto = "name name name name name_2 name_2 name_2 name_2";
    StringBuilder val = new StringBuilder();
    for (int j = 0; j < 16; j++) {
      val.append(proto);
      val.append(" ");
    }
    String pattern = val.toString();
    for (int i = 0; i < Lucene50PostingsFormat.BLOCK_SIZE/2; i++) {
      iw.addDocument(newDocument(pattern));
    }
  }
}
