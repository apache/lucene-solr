package org.apache.lucene.codecs.lucene40;

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
    Lucene40CompoundReader csw = new Lucene40CompoundReader(newDir, "d.cfs", newIOContext(random()), true);
    Lucene40CompoundReader nested = new Lucene40CompoundReader(newDir, "b.cfs", newIOContext(random()), true);
    IndexOutput out = nested.createOutput("b.xyz", newIOContext(random()));
    IndexOutput out1 = nested.createOutput("b_1.xyz", newIOContext(random()));
    out.writeInt(0);
    out1.writeInt(1);
    out.close();
    out1.close();
    nested.close();
    newDir.copy(csw, "b.cfs", "b.cfs", newIOContext(random()));
    newDir.copy(csw, "b.cfe", "b.cfe", newIOContext(random()));
    newDir.deleteFile("b.cfs");
    newDir.deleteFile("b.cfe");
    csw.close();
    
    assertEquals(2, newDir.listAll().length);
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

  /** Creates a file of the specified size with sequential data. The first
   *  byte is written as the start byte provided. All subsequent bytes are
   *  computed as start + offset where offset is the number of the byte.
   */
  private void createSequenceFile(Directory dir, String name, byte start, int size) throws IOException {
    IndexOutput os = dir.createOutput(name, newIOContext(random()));
    for (int i=0; i < size; i++) {
      os.writeByte(start);
      start ++;
    }
    os.close();
  }
}
