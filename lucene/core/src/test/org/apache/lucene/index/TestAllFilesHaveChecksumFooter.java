package org.apache.lucene.index;

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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Test that a plain default puts CRC32 footers in all files.
 */
public class TestAllFilesHaveChecksumFooter extends LuceneTestCase {
  public void test() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setCodec(TestUtil.getDefaultCodec());
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    // these fields should sometimes get term vectors, etc
    Field idField = newStringField("id", "", Field.Store.NO);
    Field bodyField = newTextField("body", "", Field.Store.NO);
    Field dvField = new NumericDocValuesField("dv", 5);
    doc.add(idField);
    doc.add(bodyField);
    doc.add(dvField);
    for (int i = 0; i < 100; i++) {
      idField.setStringValue(Integer.toString(i));
      bodyField.setStringValue(TestUtil.randomUnicodeString(random()));
      riw.addDocument(doc);
      if (random().nextInt(7) == 0) {
        riw.commit();
      }
      if (random().nextInt(20) == 0) {
        riw.deleteDocuments(new Term("id", Integer.toString(i)));
      }
    }
    riw.close();
    checkFooters(dir);
    dir.close();
  }
  
  private void checkFooters(Directory dir) throws IOException {
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    checkFooter(dir, sis.getSegmentsFileName());
    
    for (SegmentCommitInfo si : sis) {
      for (String file : si.files()) {
        checkFooter(dir, file);
      }
      if (si.info.getUseCompoundFile()) {
        try (Directory cfsDir = si.info.getCodec().compoundFormat().getCompoundReader(dir, si.info, newIOContext(random()))) {
          for (String cfsFile : cfsDir.listAll()) {
            checkFooter(cfsDir, cfsFile);
          }
        }
      }
    }
  }
  
  private void checkFooter(Directory dir, String file) throws IOException {
    try (IndexInput in = dir.openInput(file, newIOContext(random()))) {
      CodecUtil.checksumEntireFile(in);
    }
  }
}
