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
import org.apache.lucene.codecs.lucene410.Lucene410Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Test that a plain default puts codec headers in all files.
 */
public class TestAllFilesHaveCodecHeader extends LuceneTestCase {
  public void test() throws Exception {
    Directory dir = newDirectory();

    if (dir instanceof MockDirectoryWrapper) {
      // Else we might remove .cfe but not the corresponding .cfs, causing false exc when trying to verify headers:
      ((MockDirectoryWrapper) dir).setEnableVirusScanner(false);
    }

    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setCodec(new Lucene410Codec());
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
      // TODO: we should make a new format with a clean header...
      // if (random().nextInt(20) == 0) {
      //  riw.deleteDocuments(new Term("id", Integer.toString(i)));
      // }
    }
    riw.close();
    checkHeaders(dir);
    dir.close();
  }
  
  private void checkHeaders(Directory dir) throws IOException {
    for (String file : dir.listAll()) {
      if (file.equals(IndexWriter.WRITE_LOCK_NAME)) {
        continue; // write.lock has no header, thats ok
      }
      if (file.endsWith(IndexFileNames.COMPOUND_FILE_EXTENSION)) {
        CompoundFileDirectory cfsDir = new CompoundFileDirectory(dir, file, newIOContext(random()), false);
        checkHeaders(cfsDir); // recurse into cfs
        cfsDir.close();
      }
      IndexInput in = null;
      boolean success = false;
      try {
        in = dir.openInput(file, newIOContext(random()));
        int val = in.readInt();
        assertEquals(file + " has no codec header, instead found: " + val, CodecUtil.CODEC_MAGIC, val);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(in);
        } else {
          IOUtils.closeWhileHandlingException(in);
        }
      }
    }
  }
}
