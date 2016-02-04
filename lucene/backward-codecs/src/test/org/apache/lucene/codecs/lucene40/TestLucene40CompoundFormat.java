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
package org.apache.lucene.codecs.lucene40;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BaseCompoundFormatTestCase;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class TestLucene40CompoundFormat extends BaseCompoundFormatTestCase {
  private final Codec codec = new Lucene40RWCodec();

  @Override
  protected Codec getCodec() {
    return codec;
  }
  
  // LUCENE-3382 test that delegate compound files correctly.
  public void testCompoundFileAppendTwice() throws IOException {
    Directory newDir = newFSDirectory(createTempDir("testCompoundFileAppendTwice"));
    Directory csw = new Lucene40CompoundReader(newDir, "d.cfs", newIOContext(random()), true);
    createSequenceFile(newDir, "d1", (byte) 0, 15);
    IndexOutput out = csw.createOutput("d.xyz", newIOContext(random()));
    out.writeInt(0);
    out.close();
    assertEquals(1, csw.listAll().length);
    assertEquals("d.xyz", csw.listAll()[0]);
   
    csw.close();

    Directory cfr = new Lucene40CompoundReader(newDir, "d.cfs", newIOContext(random()), false);
    assertEquals(1, cfr.listAll().length);
    assertEquals("d.xyz", cfr.listAll()[0]);
    cfr.close();
    newDir.close();
  }
  
  public void testReadNestedCFP() throws IOException {
    Directory newDir = newDirectory();
    // manually manipulates directory
    if (newDir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)newDir).setEnableVirusScanner(false);
    }
    int size = newDir.listAll().length;
    Lucene40CompoundReader csw = new Lucene40CompoundReader(newDir, "d.cfs", newIOContext(random()), true);
    Lucene40CompoundReader nested = new Lucene40CompoundReader(newDir, "b.cfs", newIOContext(random()), true);
    IndexOutput out = nested.createOutput("b.xyz", newIOContext(random()));
    IndexOutput out1 = nested.createOutput("b_1.xyz", newIOContext(random()));
    out.writeInt(0);
    out1.writeInt(1);
    out.close();
    out1.close();
    nested.close();
    csw.copyFrom(newDir, "b.cfs", "b.cfs", newIOContext(random()));
    csw.copyFrom(newDir, "b.cfe", "b.cfe", newIOContext(random()));
    newDir.deleteFile("b.cfs");
    newDir.deleteFile("b.cfe");
    csw.close();
    
    assertEquals(size+2, newDir.listAll().length);
    csw = new Lucene40CompoundReader(newDir, "d.cfs", newIOContext(random()), false);
    
    assertEquals(2, csw.listAll().length);
    nested = new Lucene40CompoundReader(csw, "b.cfs", newIOContext(random()), false);
    
    assertEquals(2, nested.listAll().length);
    IndexInput openInput = nested.openInput("b.xyz", newIOContext(random()));
    assertEquals(0, openInput.readInt());
    openInput.close();
    openInput = nested.openInput("b_1.xyz", newIOContext(random()));
    assertEquals(1, openInput.readInt());
    openInput.close();
    nested.close();
    csw.close();
    newDir.close();
  }
  
  public void testAppend() throws IOException {
    Directory dir = newDirectory();
    Directory newDir = newDirectory();
    Directory csw = new Lucene40CompoundReader(newDir, "d.cfs", newIOContext(random()), true);
    int size = 5 + random().nextInt(128);
    for (int j = 0; j < 2; j++) {
      IndexOutput os = csw.createOutput("seg_" + j + "_foo.txt", newIOContext(random()));
      for (int i = 0; i < size; i++) {
        os.writeInt(i*j);
      }
      os.close();
      assertTrue(Arrays.asList(newDir.listAll()).contains("d.cfs"));
    }
    createSequenceFile(dir, "d1", (byte) 0, 15);
    csw.copyFrom(dir, "d1", "d1", newIOContext(random()));
    assertTrue(Arrays.asList(newDir.listAll()).contains("d.cfs"));
    csw.close();
    
    Directory csr = new Lucene40CompoundReader(newDir, "d.cfs", newIOContext(random()), false);
    for (int j = 0; j < 2; j++) {
      IndexInput openInput = csr.openInput("seg_" + j + "_foo.txt", newIOContext(random()));
      assertEquals(size * 4, openInput.length());
      for (int i = 0; i < size; i++) {
        assertEquals(i*j, openInput.readInt());
      }
      
      openInput.close();
    }
    IndexInput expected = dir.openInput("d1", newIOContext(random()));
    IndexInput actual = csr.openInput("d1", newIOContext(random()));
    assertSameStreams("d1", expected, actual);
    assertSameSeekBehavior("d1", expected, actual);
    expected.close();
    actual.close();
    csr.close();
    newDir.close();
    dir.close();
  }
  
  public void testAppendTwice() throws IOException {
    Directory newDir = newDirectory();
    Directory csw = new Lucene40CompoundReader(newDir, "d.cfs", newIOContext(random()), true);
    createSequenceFile(newDir, "d1", (byte) 0, 15);
    IndexOutput out = csw.createOutput("d.xyz", newIOContext(random()));
    out.writeInt(0);
    out.close();
    assertEquals(1, csw.listAll().length);
    assertEquals("d.xyz", csw.listAll()[0]);
    
    csw.close();
    
    Directory cfr = new Lucene40CompoundReader(newDir, "d.cfs", newIOContext(random()), false);
    assertEquals(1, cfr.listAll().length);
    assertEquals("d.xyz", cfr.listAll()[0]);
    cfr.close();
    newDir.close();
  }
}
