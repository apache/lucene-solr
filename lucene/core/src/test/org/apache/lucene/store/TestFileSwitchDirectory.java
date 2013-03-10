package org.apache.lucene.store;

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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TestIndexWriterReader;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestFileSwitchDirectory extends LuceneTestCase {
  /**
   * Test if writing doc stores to disk and everything else to ram works.
   */
  public void testBasic() throws IOException {
    Set<String> fileExtensions = new HashSet<String>();
    fileExtensions.add(Lucene40StoredFieldsWriter.FIELDS_EXTENSION);
    fileExtensions.add(Lucene40StoredFieldsWriter.FIELDS_INDEX_EXTENSION);
    
    MockDirectoryWrapper primaryDir = new MockDirectoryWrapper(random(), new RAMDirectory());
    primaryDir.setCheckIndexOnClose(false); // only part of an index
    MockDirectoryWrapper secondaryDir = new MockDirectoryWrapper(random(), new RAMDirectory());
    secondaryDir.setCheckIndexOnClose(false); // only part of an index
    
    FileSwitchDirectory fsd = new FileSwitchDirectory(fileExtensions, primaryDir, secondaryDir, true);
    // for now we wire Lucene40Codec because we rely upon its specific impl
    IndexWriter writer = new IndexWriter(
        fsd,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
            setMergePolicy(newLogMergePolicy(false)).setCodec(Codec.forName("Lucene40"))
    );
    TestIndexWriterReader.createIndexNoClose(true, "ram", writer);
    IndexReader reader = DirectoryReader.open(writer, true);
    assertEquals(100, reader.maxDoc());
    writer.commit();
    // we should see only fdx,fdt files here
    String[] files = primaryDir.listAll();
    assertTrue(files.length > 0);
    for (int x=0; x < files.length; x++) {
      String ext = FileSwitchDirectory.getExtension(files[x]);
      assertTrue(fileExtensions.contains(ext));
    }
    files = secondaryDir.listAll();
    assertTrue(files.length > 0);
    // we should not see fdx,fdt files here
    for (int x=0; x < files.length; x++) {
      String ext = FileSwitchDirectory.getExtension(files[x]);
      assertFalse(fileExtensions.contains(ext));
    }
    reader.close();
    writer.close();

    files = fsd.listAll();
    for(int i=0;i<files.length;i++) {
      assertNotNull(files[i]);
    }
    fsd.close();
  }
  
  private Directory newFSSwitchDirectory(Set<String> primaryExtensions) throws IOException {
    File primDir = _TestUtil.getTempDir("foo");
    File secondDir = _TestUtil.getTempDir("bar");
    return newFSSwitchDirectory(primDir, secondDir, primaryExtensions);
  }

  private Directory newFSSwitchDirectory(File aDir, File bDir, Set<String> primaryExtensions) throws IOException {
    Directory a = new SimpleFSDirectory(aDir);
    Directory b = new SimpleFSDirectory(bDir);
    FileSwitchDirectory switchDir = new FileSwitchDirectory(primaryExtensions, a, b, true);
    return new MockDirectoryWrapper(random(), switchDir);
  }
  
  // LUCENE-3380 -- make sure we get exception if the directory really does not exist.
  public void testNoDir() throws Throwable {
    File primDir = _TestUtil.getTempDir("foo");
    File secondDir = _TestUtil.getTempDir("bar");
    _TestUtil.rmDir(primDir);
    _TestUtil.rmDir(secondDir);
    Directory dir = newFSSwitchDirectory(primDir, secondDir, Collections.<String>emptySet());
    try {
      DirectoryReader.open(dir);
      fail("did not hit expected exception");
    } catch (NoSuchDirectoryException nsde) {
      // expected
    }
    dir.close();
  }
  
  // LUCENE-3380 test that we can add a file, and then when we call list() we get it back
  public void testDirectoryFilter() throws IOException {
    Directory dir = newFSSwitchDirectory(Collections.<String>emptySet());
    String name = "file";
    try {
      dir.createOutput(name, newIOContext(random())).close();
      assertTrue(dir.fileExists(name));
      assertTrue(Arrays.asList(dir.listAll()).contains(name));
    } finally {
      dir.close();
    }
  }
  
  // LUCENE-3380 test that delegate compound files correctly.
  public void testCompoundFileAppendTwice() throws IOException {
    Directory newDir = newFSSwitchDirectory(Collections.singleton("cfs"));
    CompoundFileDirectory csw = new CompoundFileDirectory(newDir, "d.cfs", newIOContext(random()), true);
    createSequenceFile(newDir, "d1", (byte) 0, 15);
    IndexOutput out = csw.createOutput("d.xyz", newIOContext(random()));
    out.writeInt(0);
    try {
      newDir.copy(csw, "d1", "d1", newIOContext(random()));
      fail("file does already exist");
    } catch (IllegalArgumentException e) {
      //
    }
    out.close();
    assertEquals(1, csw.listAll().length);
    assertEquals("d.xyz", csw.listAll()[0]);
   
    csw.close();

    CompoundFileDirectory cfr = new CompoundFileDirectory(newDir, "d.cfs", newIOContext(random()), false);
    assertEquals(1, cfr.listAll().length);
    assertEquals("d.xyz", cfr.listAll()[0]);
    cfr.close();
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
