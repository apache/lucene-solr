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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Test Indexing/IndexWriter with points */
public class TestPointValues extends LuceneTestCase {

  // Suddenly add points to an existing field:
  public void testUpgradeFieldToPoints() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(newStringField("dim", "foo", Field.Store.NO));
    w.addDocument(doc);
    w.close();
    
    iwc = newIndexWriterConfig();
    w = new IndexWriter(dir, iwc);
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.close();
    dir.close();
  }

  // Illegal schema change tests:

  public void testIllegalDimChangeOneDoc() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoWriters() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeViaAddIndexesDirectory() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    w.addDocument(doc);
    try {
      w.addIndexes(new Directory[] {dir});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point dimension count from 2 to 1 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(w, dir, dir2);
  }

  public void testIllegalDimChangeViaAddIndexesCodecReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      w.addIndexes(new CodecReader[] {getOnlySegmentReader(r)});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point dimension count from 2 to 1 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalDimChangeViaAddIndexesSlowCodecReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      TestUtil.addIndexesSlowly(w, r);
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point dimension count from 2 to 1 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalNumBytesChangeOneDoc() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    doc.add(new BinaryPoint("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoWriters() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeViaAddIndexesDirectory() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    w.addDocument(doc);
    try {
      w.addIndexes(new Directory[] {dir});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point numBytes from 6 to 4 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(w, dir, dir2);
  }

  public void testIllegalNumBytesChangeViaAddIndexesCodecReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      w.addIndexes(new CodecReader[] {getOnlySegmentReader(r)});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point numBytes from 6 to 4 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalNumBytesChangeViaAddIndexesSlowCodecReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      TestUtil.addIndexesSlowly(w, r);
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point numBytes from 6 to 4 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalTooManyBytes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[PointValues.MAX_NUM_BYTES+1]));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    doc = new Document();
    doc.add(new IntPoint("dim", 17));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testIllegalTooManyDimensions() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    byte[][] values = new byte[PointValues.MAX_DIMENSIONS+1][];
    for(int i=0;i<values.length;i++) {
      values[i] = new byte[4];
    }
    doc.add(new BinaryPoint("dim", values));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    doc = new Document();
    doc.add(new IntPoint("dim", 17));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // Write point values, one segment with Lucene60, another with SimpleText, then forceMerge with SimpleText
  public void testDifferentCodecs1() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene60"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new IntPoint("int", 1));
    w.addDocument(doc);
    w.close();
    
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("SimpleText"));
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new IntPoint("int", 1));
    w.addDocument(doc);

    w.forceMerge(1);
    w.close();
    dir.close();
  }

  // Write point values, one segment with Lucene60, another with SimpleText, then forceMerge with Lucene60
  public void testDifferentCodecs2() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("SimpleText"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new IntPoint("int", 1));
    w.addDocument(doc);
    w.close();
    
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene60"));
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new IntPoint("int", 1));
    w.addDocument(doc);

    w.forceMerge(1);
    w.close();
    dir.close();
  }

  public void testInvalidIntPointUsage() throws Exception {
    IntPoint field = new IntPoint("field", 17, 42);
    try {
      field.setIntValue(14);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // good
    }

    try {
      field.numericValue();
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // good
    }
  }

  public void testInvalidLongPointUsage() throws Exception {
    LongPoint field = new LongPoint("field", 17, 42);
    try {
      field.setLongValue(14);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // good
    }

    try {
      field.numericValue();
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // good
    }
  }

  public void testInvalidFloatPointUsage() throws Exception {
    FloatPoint field = new FloatPoint("field", 17, 42);
    try {
      field.setFloatValue(14);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // good
    }

    try {
      field.numericValue();
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // good
    }
  }

  public void testInvalidDoublePointUsage() throws Exception {
    DoublePoint field = new DoublePoint("field", 17, 42);
    try {
      field.setDoubleValue(14);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // good
    }

    try {
      field.numericValue();
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // good
    }
  }
}
