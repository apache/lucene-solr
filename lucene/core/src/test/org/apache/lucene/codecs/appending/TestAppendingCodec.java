package org.apache.lucene.codecs.appending;

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
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.appending.AppendingCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;

public class TestAppendingCodec extends LuceneTestCase {
  
    private static class AppendingIndexOutputWrapper extends IndexOutput {
    IndexOutput wrapped;
    
    public AppendingIndexOutputWrapper(IndexOutput wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public void close() throws IOException {
      wrapped.close();
    }

    @Override
    public void flush() throws IOException {
      wrapped.flush();
    }

    @Override
    public long getFilePointer() {
      return wrapped.getFilePointer();
    }

    @Override
    public long length() throws IOException {
      return wrapped.length();
    }

    @Override
    public void seek(long pos) throws IOException {
      throw new UnsupportedOperationException("seek() is unsupported");
    }

    @Override
    public void writeByte(byte b) throws IOException {
      wrapped.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      wrapped.writeBytes(b, offset, length);
    }
    
  }
  
  @SuppressWarnings("serial")
  private static class AppendingRAMDirectory extends MockDirectoryWrapper {

    public AppendingRAMDirectory(Random random, Directory delegate) {
      super(random, delegate);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      return new AppendingIndexOutputWrapper(super.createOutput(name, context));
    }
    
  }
  
  private static final String text = "the quick brown fox jumped over the lazy dog";

  public void testCodec() throws Exception {
    Directory dir = new AppendingRAMDirectory(random(), new RAMDirectory());
    IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_40, new MockAnalyzer(random()));
    
    cfg.setCodec(new AppendingCodec());
    ((TieredMergePolicy)cfg.getMergePolicy()).setUseCompoundFile(false);
    IndexWriter writer = new IndexWriter(dir, cfg);
    Document doc = new Document();
    FieldType storedTextType = new FieldType(TextField.TYPE_STORED);
    storedTextType.setStoreTermVectors(true);
    storedTextType.setStoreTermVectorPositions(true);
    storedTextType.setStoreTermVectorOffsets(true);
    doc.add(newField("f", text, storedTextType));
    writer.addDocument(doc);
    writer.commit();
    writer.addDocument(doc);
    writer.forceMerge(1);
    writer.close();
    IndexReader reader = IndexReader.open(dir, 1);
    assertEquals(2, reader.numDocs());
    Document doc2 = reader.document(0);
    assertEquals(text, doc2.get("f"));
    Fields fields = MultiFields.getFields(reader);
    Terms terms = fields.terms("f");
    assertNotNull(terms);
    TermsEnum te = terms.iterator(null);
    assertEquals(SeekStatus.FOUND, te.seekCeil(new BytesRef("quick")));
    assertEquals(SeekStatus.FOUND, te.seekCeil(new BytesRef("brown")));
    assertEquals(SeekStatus.FOUND, te.seekCeil(new BytesRef("fox")));
    assertEquals(SeekStatus.FOUND, te.seekCeil(new BytesRef("jumped")));
    assertEquals(SeekStatus.FOUND, te.seekCeil(new BytesRef("over")));
    assertEquals(SeekStatus.FOUND, te.seekCeil(new BytesRef("lazy")));
    assertEquals(SeekStatus.FOUND, te.seekCeil(new BytesRef("dog")));
    assertEquals(SeekStatus.FOUND, te.seekCeil(new BytesRef("the")));
    DocsEnum de = te.docs(null, null, true);
    assertTrue(de.advance(0) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(2, de.freq());
    assertTrue(de.advance(1) != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(de.advance(2) == DocIdSetIterator.NO_MORE_DOCS);
    reader.close();
  }
  
  public void testCompoundFile() throws Exception {
    Directory dir = new AppendingRAMDirectory(random(), new RAMDirectory());
    IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_40, new MockAnalyzer(random()));
    TieredMergePolicy mp = new TieredMergePolicy();
    mp.setUseCompoundFile(true);
    mp.setNoCFSRatio(1.0);
    cfg.setMergePolicy(mp);
    cfg.setCodec(new AppendingCodec());
    IndexWriter writer = new IndexWriter(dir, cfg);
    Document doc = new Document();
    writer.addDocument(doc);
    writer.close();
    assertTrue(dir.fileExists("_0.cfs"));
    dir.close();
  }
}
