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

import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;

/** 
 * Setup a large compound file with a number of components, each of
 * which is a sequential file (so that we can easily tell that we are
 * reading in the right byte). The methods sets up 20 files - f0 to f19,
 * the size of each file is 1000 bytes.
 */
public class TestCompoundFile2 extends LuceneTestCase {
  private Directory dir;
  byte id[];
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    id = StringHelper.randomId();
    dir = newDirectory();
    CompoundFileDirectory cw = new CompoundFileDirectory(id, dir, "f.comp", newIOContext(random()), true);
    for (int i=0; i<20; i++) {
      TestCompoundFile.createSequenceFile(dir, "f" + i, (byte) 0, 2000);
      String fileName = "f" + i;
      dir.copy(cw, fileName, fileName, newIOContext(random()));
    }
    cw.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }
  
  public void testClonedStreamsClosing() throws IOException {
    CompoundFileDirectory cr = new CompoundFileDirectory(id, dir, "f.comp", newIOContext(random()), false);
    
    // basic clone
    IndexInput expected = dir.openInput("f11", newIOContext(random()));
    
    IndexInput one = cr.openInput("f11", newIOContext(random()));
    
    IndexInput two = one.clone();
    
    TestCompoundFile.assertSameStreams("basic clone one", expected, one);
    expected.seek(0);
    TestCompoundFile.assertSameStreams("basic clone two", expected, two);
    
    // Now close the first stream
    one.close();
    
    // The following should really fail since we couldn't expect to
    // access a file once close has been called on it (regardless of
    // buffering and/or clone magic)
    expected.seek(0);
    two.seek(0);
    TestCompoundFile.assertSameStreams("basic clone two/2", expected, two);
    
    // Now close the compound reader
    cr.close();
    
    // The following may also fail since the compound stream is closed
    expected.seek(0);
    two.seek(0);
    //assertSameStreams("basic clone two/3", expected, two);
    
    // Now close the second clone
    two.close();
    expected.seek(0);
    //assertSameStreams("basic clone two/4", expected, two);
    
    expected.close();
  }
  
  /** This test opens two files from a compound stream and verifies that
   *  their file positions are independent of each other.
   */
  public void testRandomAccess() throws IOException {
    CompoundFileDirectory cr = new CompoundFileDirectory(id, dir, "f.comp", newIOContext(random()), false);
    
    // Open two files
    IndexInput e1 = dir.openInput("f11", newIOContext(random()));
    IndexInput e2 = dir.openInput("f3", newIOContext(random()));
    
    IndexInput a1 = cr.openInput("f11", newIOContext(random()));
    IndexInput a2 = dir.openInput("f3", newIOContext(random()));
    
    // Seek the first pair
    e1.seek(100);
    a1.seek(100);
    assertEquals(100, e1.getFilePointer());
    assertEquals(100, a1.getFilePointer());
    byte be1 = e1.readByte();
    byte ba1 = a1.readByte();
    assertEquals(be1, ba1);
    
    // Now seek the second pair
    e2.seek(1027);
    a2.seek(1027);
    assertEquals(1027, e2.getFilePointer());
    assertEquals(1027, a2.getFilePointer());
    byte be2 = e2.readByte();
    byte ba2 = a2.readByte();
    assertEquals(be2, ba2);
    
    // Now make sure the first one didn't move
    assertEquals(101, e1.getFilePointer());
    assertEquals(101, a1.getFilePointer());
    be1 = e1.readByte();
    ba1 = a1.readByte();
    assertEquals(be1, ba1);
    
    // Now more the first one again, past the buffer length
    e1.seek(1910);
    a1.seek(1910);
    assertEquals(1910, e1.getFilePointer());
    assertEquals(1910, a1.getFilePointer());
    be1 = e1.readByte();
    ba1 = a1.readByte();
    assertEquals(be1, ba1);
    
    // Now make sure the second set didn't move
    assertEquals(1028, e2.getFilePointer());
    assertEquals(1028, a2.getFilePointer());
    be2 = e2.readByte();
    ba2 = a2.readByte();
    assertEquals(be2, ba2);
    
    // Move the second set back, again cross the buffer size
    e2.seek(17);
    a2.seek(17);
    assertEquals(17, e2.getFilePointer());
    assertEquals(17, a2.getFilePointer());
    be2 = e2.readByte();
    ba2 = a2.readByte();
    assertEquals(be2, ba2);
    
    // Finally, make sure the first set didn't move
    // Now make sure the first one didn't move
    assertEquals(1911, e1.getFilePointer());
    assertEquals(1911, a1.getFilePointer());
    be1 = e1.readByte();
    ba1 = a1.readByte();
    assertEquals(be1, ba1);
    
    e1.close();
    e2.close();
    a1.close();
    a2.close();
    cr.close();
  }
  
  /** This test opens two files from a compound stream and verifies that
   *  their file positions are independent of each other.
   */
  public void testRandomAccessClones() throws IOException {
    CompoundFileDirectory cr = new CompoundFileDirectory(id, dir, "f.comp", newIOContext(random()), false);
    
    // Open two files
    IndexInput e1 = cr.openInput("f11", newIOContext(random()));
    IndexInput e2 = cr.openInput("f3", newIOContext(random()));
    
    IndexInput a1 = e1.clone();
    IndexInput a2 = e2.clone();
    
    // Seek the first pair
    e1.seek(100);
    a1.seek(100);
    assertEquals(100, e1.getFilePointer());
    assertEquals(100, a1.getFilePointer());
    byte be1 = e1.readByte();
    byte ba1 = a1.readByte();
    assertEquals(be1, ba1);
    
    // Now seek the second pair
    e2.seek(1027);
    a2.seek(1027);
    assertEquals(1027, e2.getFilePointer());
    assertEquals(1027, a2.getFilePointer());
    byte be2 = e2.readByte();
    byte ba2 = a2.readByte();
    assertEquals(be2, ba2);
    
    // Now make sure the first one didn't move
    assertEquals(101, e1.getFilePointer());
    assertEquals(101, a1.getFilePointer());
    be1 = e1.readByte();
    ba1 = a1.readByte();
    assertEquals(be1, ba1);
    
    // Now more the first one again, past the buffer length
    e1.seek(1910);
    a1.seek(1910);
    assertEquals(1910, e1.getFilePointer());
    assertEquals(1910, a1.getFilePointer());
    be1 = e1.readByte();
    ba1 = a1.readByte();
    assertEquals(be1, ba1);
    
    // Now make sure the second set didn't move
    assertEquals(1028, e2.getFilePointer());
    assertEquals(1028, a2.getFilePointer());
    be2 = e2.readByte();
    ba2 = a2.readByte();
    assertEquals(be2, ba2);
    
    // Move the second set back, again cross the buffer size
    e2.seek(17);
    a2.seek(17);
    assertEquals(17, e2.getFilePointer());
    assertEquals(17, a2.getFilePointer());
    be2 = e2.readByte();
    ba2 = a2.readByte();
    assertEquals(be2, ba2);
    
    // Finally, make sure the first set didn't move
    // Now make sure the first one didn't move
    assertEquals(1911, e1.getFilePointer());
    assertEquals(1911, a1.getFilePointer());
    be1 = e1.readByte();
    ba1 = a1.readByte();
    assertEquals(be1, ba1);
    
    e1.close();
    e2.close();
    a1.close();
    a2.close();
    cr.close();
  }
  
  public void testFileNotFound() throws IOException {
    CompoundFileDirectory cr = new CompoundFileDirectory(id, dir, "f.comp", newIOContext(random()), false);
    
    // Open two files
    try {
      cr.openInput("bogus", newIOContext(random()));
      fail("File not found");
    } catch (IOException e) {
      /* success */
      //System.out.println("SUCCESS: File Not Found: " + e);
    }
    
    cr.close();
  }
  
  public void testReadPastEOF() throws IOException {
    CompoundFileDirectory cr = new CompoundFileDirectory(id, dir, "f.comp", newIOContext(random()), false);
    IndexInput is = cr.openInput("f2", newIOContext(random()));
    is.seek(is.length() - 10);
    byte b[] = new byte[100];
    is.readBytes(b, 0, 10);
    
    try {
      is.readByte();
      fail("Single byte read past end of file");
    } catch (IOException e) {
      /* success */
      //System.out.println("SUCCESS: single byte read past end of file: " + e);
    }
    
    is.seek(is.length() - 10);
    try {
      is.readBytes(b, 0, 50);
      fail("Block read past end of file");
    } catch (IOException e) {
      /* success */
      //System.out.println("SUCCESS: block read past end of file: " + e);
    }
    
    is.close();
    cr.close();
  }
}
