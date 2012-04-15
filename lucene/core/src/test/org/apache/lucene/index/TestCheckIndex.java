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
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.Constants;

public class TestCheckIndex extends LuceneTestCase {

  public void testDeletedDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(2));
    for(int i=0;i<19;i++) {
      Document doc = new Document();
      FieldType customType = new FieldType(TextField.TYPE_STORED);
      customType.setStoreTermVectors(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectorOffsets(true);
      doc.add(newField("field", "aaa"+i, customType));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.commit();
    writer.deleteDocuments(new Term("field","aaa5"));
    writer.close();

    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    CheckIndex checker = new CheckIndex(dir);
    checker.setInfoStream(new PrintStream(bos));
    if (VERBOSE) checker.setInfoStream(System.out);
    CheckIndex.Status indexStatus = checker.checkIndex();
    if (indexStatus.clean == false) {
      System.out.println("CheckIndex failed");
      System.out.println(bos.toString());
      fail();
    }
    
    final CheckIndex.Status.SegmentInfoStatus seg = indexStatus.segmentInfos.get(0);
    assertTrue(seg.openReaderPassed);

    assertNotNull(seg.diagnostics);
    
    assertNotNull(seg.fieldNormStatus);
    assertNull(seg.fieldNormStatus.error);
    assertEquals(1, seg.fieldNormStatus.totFields);

    assertNotNull(seg.termIndexStatus);
    assertNull(seg.termIndexStatus.error);
    assertEquals(19, seg.termIndexStatus.termCount);
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
    final List<String> onlySegments = new ArrayList<String>();
    onlySegments.add("_0");
    
    assertTrue(checker.checkIndex(onlySegments).clean == true);
    dir.close();
  }

  public void testLuceneConstantVersion() throws IOException {
    // common-build.xml sets lucene.version
    final String version = System.getProperty("lucene.version");
    assertNotNull( "null version", version);
    assertTrue("Invalid version: "+version,
               version.equals(Constants.LUCENE_MAIN_VERSION+"-SNAPSHOT") ||
               version.equals(Constants.LUCENE_MAIN_VERSION));
    assertTrue(version + " should start with: "+Constants.LUCENE_VERSION,
               Constants.LUCENE_VERSION.startsWith(version));
  }
}
