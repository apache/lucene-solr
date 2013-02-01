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
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Test that a plain default puts codec headers in all files.
 */
public class TestAllFilesHaveCodecHeader extends LuceneTestCase {
  public void test() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setCodec(new Lucene42Codec());
    // riw should sometimes create docvalues fields, etc
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    // these fields should sometimes get term vectors, etc
    Field idField = newStringField("id", "", Field.Store.NO);
    Field bodyField = newTextField("body", "", Field.Store.NO);
    doc.add(idField);
    doc.add(bodyField);
    for (int i = 0; i < 100; i++) {
      idField.setStringValue(Integer.toString(i));
      bodyField.setStringValue(_TestUtil.randomUnicodeString(random()));
      riw.addDocument(doc);
      if (random().nextInt(7) == 0) {
        riw.commit();
      }
    }
    riw.close();
    checkHeaders(dir);
    dir.close();
  }
  
  private void checkHeaders(Directory dir) throws IOException {
    for (String file : dir.listAll()) {
      if (file.equals(IndexFileNames.SEGMENTS_GEN)) {
        continue; // segments.gen has no header, thats ok
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
