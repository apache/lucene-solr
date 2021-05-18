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
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.BaseStoredFieldsFormatTestCase;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

public class TestCompressingStoredFieldsFormat extends BaseStoredFieldsFormatTestCase {

  static final long SECOND = 1000L;
  static final long HOUR = 60 * 60 * SECOND;
  static final long DAY = 24 * HOUR;

  @Override
  protected Codec getCodec() {
    if (TEST_NIGHTLY) {
      return CompressingCodec.randomInstance(random());
    } else {
      return CompressingCodec.reasonableInstance(random());
    }
  }

  public void testDeletePartiallyWrittenFilesIfAbort() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomNumbers.randomIntBetween(random(), 2, 30));
    iwConf.setCodec(getCodec());
    // disable CFS because this test checks file names
    iwConf.setMergePolicy(newLogMergePolicy(false));
    iwConf.setUseCompoundFile(false);

    // Cannot use RIW because this test wants CFS to stay off:
    IndexWriter iw = new IndexWriter(dir, iwConf);

    final Document validDoc = new Document();
    validDoc.add(new IntPoint("id", 0));
    validDoc.add(new StoredField("id", 0));
    iw.addDocument(validDoc);
    iw.commit();
    
    // make sure that #writeField will fail to trigger an abort
    final Document invalidDoc = new Document();
    FieldType fieldType = new FieldType();
    fieldType.setStored(true);
    invalidDoc.add(new Field("invalid", fieldType) {
      
      @Override
      public String stringValue() {
        // TODO: really bad & scary that this causes IW to
        // abort the segment!!  We should fix this.
        return null;
      }
      
    });
    
    try {
      iw.addDocument(invalidDoc);
      iw.commit();
    } catch(IllegalArgumentException iae) {
      // expected
      assertEquals(iae, iw.getTragicException());
    }
    // Writer should be closed by tragedy
    assertFalse(iw.isOpen());
    dir.close();
  }

  public void testZFloat() throws Exception {
    byte buffer[] = new byte[5]; // we never need more than 5 bytes
    ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
    ByteArrayDataInput in = new ByteArrayDataInput(buffer);

    // round-trip small integer values
    for (int i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
      float f = (float) i;
      CompressingStoredFieldsWriter.writeZFloat(out, f);
      in.reset(buffer, 0, out.getPosition());
      float g = CompressingStoredFieldsReader.readZFloat(in);
      assertTrue(in.eof());
      assertEquals(Float.floatToIntBits(f), Float.floatToIntBits(g));

      // check that compression actually works
      if (i >= -1 && i <= 123) {
        assertEquals(1, out.getPosition()); // single byte compression
      }
      out.reset(buffer);
    }

    // round-trip special values
    float special[] = {
        -0.0f,
        +0.0f,
        Float.NEGATIVE_INFINITY,
        Float.POSITIVE_INFINITY,
        Float.MIN_VALUE,
        Float.MAX_VALUE,
        Float.NaN,
    };

    for (float f : special) {
      CompressingStoredFieldsWriter.writeZFloat(out, f);
      in.reset(buffer, 0, out.getPosition());
      float g = CompressingStoredFieldsReader.readZFloat(in);
      assertTrue(in.eof());
      assertEquals(Float.floatToIntBits(f), Float.floatToIntBits(g));
      out.reset(buffer);
    }

    // round-trip random values
    Random r = random();
    for (int i = 0; i < 100000; i++) {
      float f = r.nextFloat() * (random().nextInt(100) - 50);
      CompressingStoredFieldsWriter.writeZFloat(out, f);
      assertTrue("length=" + out.getPosition() + ", f=" + f, out.getPosition() <= ((Float.floatToIntBits(f) >>> 31) == 1 ? 5 : 4));
      in.reset(buffer, 0, out.getPosition());
      float g = CompressingStoredFieldsReader.readZFloat(in);
      assertTrue(in.eof());
      assertEquals(Float.floatToIntBits(f), Float.floatToIntBits(g));
      out.reset(buffer);
    }
  }

  public void testZDouble() throws Exception {
    byte buffer[] = new byte[9]; // we never need more than 9 bytes
    ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
    ByteArrayDataInput in = new ByteArrayDataInput(buffer);

    // round-trip small integer values
    for (int i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
      double x = (double) i;
      CompressingStoredFieldsWriter.writeZDouble(out, x);
      in.reset(buffer, 0, out.getPosition());
      double y = CompressingStoredFieldsReader.readZDouble(in);
      assertTrue(in.eof());
      assertEquals(Double.doubleToLongBits(x), Double.doubleToLongBits(y));

      // check that compression actually works
      if (i >= -1 && i <= 124) {
        assertEquals(1, out.getPosition()); // single byte compression
      }
      out.reset(buffer);
    }

    // round-trip special values
    double special[] = {
        -0.0d,
        +0.0d,
        Double.NEGATIVE_INFINITY,
        Double.POSITIVE_INFINITY,
        Double.MIN_VALUE,
        Double.MAX_VALUE,
        Double.NaN
    };

    for (double x : special) {
      CompressingStoredFieldsWriter.writeZDouble(out, x);
      in.reset(buffer, 0, out.getPosition());
      double y = CompressingStoredFieldsReader.readZDouble(in);
      assertTrue(in.eof());
      assertEquals(Double.doubleToLongBits(x), Double.doubleToLongBits(y));
      out.reset(buffer);
    }

    // round-trip random values
    Random r = random();
    for (int i = 0; i < 100000; i++) {
      double x = r.nextDouble() * (random().nextInt(100) - 50);
      CompressingStoredFieldsWriter.writeZDouble(out, x);
      assertTrue("length=" + out.getPosition() + ", d=" + x, out.getPosition() <= (x < 0 ? 9 : 8));
      in.reset(buffer, 0, out.getPosition());
      double y = CompressingStoredFieldsReader.readZDouble(in);
      assertTrue(in.eof());
      assertEquals(Double.doubleToLongBits(x), Double.doubleToLongBits(y));
      out.reset(buffer);
    }

    // same with floats
    for (int i = 0; i < 100000; i++) {
      double x = (double) (r.nextFloat() * (random().nextInt(100) - 50));
      CompressingStoredFieldsWriter.writeZDouble(out, x);
      assertTrue("length=" + out.getPosition() + ", d=" + x, out.getPosition() <= 5);
      in.reset(buffer, 0, out.getPosition());
      double y = CompressingStoredFieldsReader.readZDouble(in);
      assertTrue(in.eof());
      assertEquals(Double.doubleToLongBits(x), Double.doubleToLongBits(y));
      out.reset(buffer);
    }
  }

  public void testTLong() throws Exception {
    byte buffer[] = new byte[10]; // we never need more than 10 bytes
    ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
    ByteArrayDataInput in = new ByteArrayDataInput(buffer);

    // round-trip small integer values
    for (int i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
      for (long mul : new long[] {SECOND, HOUR, DAY}) {
        long l1 = (long) i * mul;
        CompressingStoredFieldsWriter.writeTLong(out, l1);
        in.reset(buffer, 0, out.getPosition());
        long l2 = CompressingStoredFieldsReader.readTLong(in);
        assertTrue(in.eof());
        assertEquals(l1, l2);

        // check that compression actually works
        if (i >= -16 && i <= 15) {
          assertEquals(1, out.getPosition()); // single byte compression
        }
        out.reset(buffer);
      }
    }

    // round-trip random values
    Random r = random();
    for (int i = 0; i < 100000; i++) {
      final int numBits = r.nextInt(65);
      long l1 = r.nextLong() & ((1L << numBits) - 1);
      switch (r.nextInt(4)) {
        case 0:
          l1 *= SECOND;
          break;
        case 1:
          l1 *= HOUR;
          break;
        case 2:
          l1 *= DAY;
          break;
        default:
          break;
      }
      CompressingStoredFieldsWriter.writeTLong(out, l1);
      in.reset(buffer, 0, out.getPosition());
      long l2 = CompressingStoredFieldsReader.readTLong(in);
      assertTrue(in.eof());
      assertEquals(l1, l2);
      out.reset(buffer);
    }
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
    iwConf.setCodec(CompressingCodec.randomInstance(random(), 4 * 1024, 4, false, 8));
    IndexWriter iw = new IndexWriter(dir, iwConf);
    DirectoryReader ir = DirectoryReader.open(iw);
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(new StoredField("text", "not very long at all"));
      iw.addDocument(doc);
      // force flush
      DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);
      assertNotNull(ir2);
      ir.close();
      ir = ir2;
      // examine dirty counts:
      for (LeafReaderContext leaf : ir2.leaves()) {
        CodecReader sr = (CodecReader) leaf.reader();
        CompressingStoredFieldsReader reader = (CompressingStoredFieldsReader)sr.getFieldsReader();
        assertTrue(reader.getNumDirtyDocs() > 0);
        assertTrue(reader.getNumDirtyDocs() < 100); // can't be gte the number of docs per chunk
        assertEquals(1, reader.getNumDirtyChunks());
      }
    }
    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    // add a single doc and merge again
    Document doc = new Document();
    doc.add(new StoredField("text", "not very long at all"));
    iw.addDocument(doc);
    iw.forceMerge(1);
    DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);
    assertNotNull(ir2);
    ir.close();
    ir = ir2;
    CodecReader sr = (CodecReader) getOnlyLeafReader(ir);
    CompressingStoredFieldsReader reader = (CompressingStoredFieldsReader)sr.getFieldsReader();
    // at most 2: the 5 chunks from 5 doc segment will be collapsed into a single chunk
    assertTrue(reader.getNumDirtyChunks() <= 2);
    ir.close();
    iw.close();
    dir.close();
  }
}
