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

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.Constants;

public class TestCheckIndex extends LuceneTestCase {

  public void testDeletedDocs() throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();
    IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, 
                                          IndexWriter.MaxFieldLength.LIMITED);      
    writer.setMaxBufferedDocs(2);
    Document doc = new Document();
    doc.add(new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    for(int i=0;i<19;i++) {
      writer.addDocument(doc);
    }
    writer.close();
    IndexReader reader = IndexReader.open(dir);
    reader.deleteDocument(5);
    reader.close();

    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    CheckIndex checker = new CheckIndex(dir);
    checker.setInfoStream(new PrintStream(bos));
    //checker.setInfoStream(System.out);
    CheckIndex.Status indexStatus = checker.checkIndex();
    if (indexStatus.clean == false) {
      System.out.println("CheckIndex failed");
      System.out.println(bos.toString());
      fail();
    }
    
    final CheckIndex.Status.SegmentInfoStatus seg = (CheckIndex.Status.SegmentInfoStatus) indexStatus.segmentInfos.get(0);
    assertTrue(seg.openReaderPassed);

    assertNotNull(seg.diagnostics);
    
    assertNotNull(seg.fieldNormStatus);
    assertNull(seg.fieldNormStatus.error);
    assertEquals(1, seg.fieldNormStatus.totFields);

    assertNotNull(seg.termIndexStatus);
    assertNull(seg.termIndexStatus.error);
    assertEquals(1, seg.termIndexStatus.termCount);
    assertEquals(19, seg.termIndexStatus.totFreq);
    assertEquals(18, seg.termIndexStatus.totPos);

    assertNotNull(seg.storedFieldStatus);
    assertNull(seg.storedFieldStatus.error);
    assertEquals(18, seg.storedFieldStatus.docCount);
    assertEquals(18, seg.storedFieldStatus.totFields);

    assertNotNull(seg.termVectorStatus);
    assertNull(seg.termVectorStatus.error);
    assertEquals(18, seg.termVectorStatus.docCount);
    assertEquals(18, seg.termVectorStatus.totVectors);

    assertTrue(seg.diagnostics.size() > 0);
    final List onlySegments = new ArrayList();
    onlySegments.add("_0");
    
    assertTrue(checker.checkIndex(onlySegments).clean == true);
  }

  /* Does not work, because compilation puts final field from Constants of 2.9 into class file:
  public void testLuceneConstantVersion() throws IOException {
    // common-build.xml sets lucene.version
    final String version = System.getProperty("lucene.version");
    assertNotNull(version);
    assertTrue(version.equals(Constants.LUCENE_MAIN_VERSION+"-dev") ||
               version.equals(Constants.LUCENE_MAIN_VERSION));
    assertTrue(Constants.LUCENE_VERSION.startsWith(version));
  }*/
}
