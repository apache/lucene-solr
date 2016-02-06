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
package org.apache.lucene.codecs.compressing;

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.BaseTermVectorsFormatTestCase;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

public class TestCompressingTermVectorsFormat extends BaseTermVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return CompressingCodec.randomInstance(random());
  }
  
  // https://issues.apache.org/jira/browse/LUCENE-5156
  public void testNoOrds() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    doc.add(new Field("foo", "this is a test", ft));
    iw.addDocument(doc);
    LeafReader ir = getOnlySegmentReader(iw.getReader());
    Terms terms = ir.getTermVector(0, "foo");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("this")));
    try {
      termsEnum.ord();
      fail();
    } catch (UnsupportedOperationException expected) {
      // expected exception
    }
    
    try {
      termsEnum.seekExact(0);
      fail();
    } catch (UnsupportedOperationException expected) {
      // expected exception
    }
    ir.close();
    iw.close();
    dir.close();
  }
  
  /**
   * writes some tiny segments with incomplete compressed blocks,
   * and ensures merge recompresses them.
   */
  public void testChunkCleanup() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMergePolicy(NoMergePolicy.INSTANCE);
    
    // we have to enforce certain things like maxDocsPerChunk to cause dirty chunks to be created
    // by this test.
    iwConf.setCodec(CompressingCodec.randomInstance(random(), 4*1024, 100, false, 8));
    IndexWriter iw = new IndexWriter(dir, iwConf);
    DirectoryReader ir = DirectoryReader.open(iw);
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
      ft.setStoreTermVectors(true);
      doc.add(new Field("text", "not very long at all", ft));
      iw.addDocument(doc);
      // force flush
      DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);
      assertNotNull(ir2);
      ir.close();
      ir = ir2;
      // examine dirty counts:
      for (LeafReaderContext leaf : ir2.leaves()) {
        CodecReader sr = (CodecReader) leaf.reader();
        CompressingTermVectorsReader reader = (CompressingTermVectorsReader)sr.getTermVectorsReader();
        assertEquals(1, reader.getNumChunks());
        assertEquals(1, reader.getNumDirtyChunks());
      }
    }
    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);
    assertNotNull(ir2);
    ir.close();
    ir = ir2;
    CodecReader sr = getOnlySegmentReader(ir);
    CompressingTermVectorsReader reader = (CompressingTermVectorsReader)sr.getTermVectorsReader();
    // we could get lucky, and have zero, but typically one.
    assertTrue(reader.getNumDirtyChunks() <= 1);
    ir.close();
    iw.close();
    dir.close();
  }
}
