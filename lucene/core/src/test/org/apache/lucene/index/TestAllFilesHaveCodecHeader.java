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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Test that a plain default puts codec headers in all files
 */
public class TestAllFilesHaveCodecHeader extends LuceneTestCase {
  public void test() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setCodec(TestUtil.getDefaultCodec());
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, conf);
    // Use LineFileDocs so we (hopefully) get most Lucene features
    // tested, e.g. IntPoint was recently added to it:
    LineFileDocs docs = new LineFileDocs(random());
    for (int i = 0; i < 100; i++) {
      riw.addDocument(docs.nextDoc());
      if (random().nextInt(7) == 0) {
        riw.commit();
      }
      if (random().nextInt(20) == 0) {
        riw.deleteDocuments(new Term("docid", Integer.toString(i)));
      }
      if (random().nextInt(15) == 0) {
        riw.updateNumericDocValue(new Term("docid", Integer.toString(i)), "docid_intDV", Long.valueOf(i));
      }
    }
    riw.close();
    checkHeaders(dir, new HashMap<String,String>());
    dir.close();
  }
  
  private void checkHeaders(Directory dir, Map<String,String> namesToExtensions) throws IOException {
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    checkHeader(dir, sis.getSegmentsFileName(), namesToExtensions, sis.getId());
    
    for (SegmentCommitInfo si : sis) {
      assertNotNull(si.info.getId());
      for (String file : si.files()) {
        checkHeader(dir, file, namesToExtensions, si.info.getId());
      }
      if (si.info.getUseCompoundFile()) {
        try (Directory cfsDir = si.info.getCodec().compoundFormat().getCompoundReader(dir, si.info, newIOContext(random()))) {
          for (String cfsFile : cfsDir.listAll()) {
            checkHeader(cfsDir, cfsFile, namesToExtensions, si.info.getId());
          }
        }
      }
    }
  }
  
  private void checkHeader(Directory dir, String file, Map<String,String> namesToExtensions, byte[] id) throws IOException {
    try (IndexInput in = dir.openInput(file, newIOContext(random()))) {
      int val = in.readInt();
      assertEquals(file + " has no codec header, instead found: " + val, CodecUtil.CODEC_MAGIC, val);
      String codecName = in.readString();
      assertFalse(codecName.isEmpty());
      String extension = IndexFileNames.getExtension(file);
      if (extension == null) {
        assertTrue(file.startsWith(IndexFileNames.SEGMENTS));
        extension = "<segments> (not a real extension, designates segments file)";
      }
      String previous = namesToExtensions.put(codecName, extension);
      if (previous != null && !previous.equals(extension)) {
        fail("extensions " + previous + " and " + extension + " share same codecName " + codecName);
      }
      // read version
      in.readInt();
      // read object id
      CodecUtil.checkIndexHeaderID(in, id);      
    }
  }
}
