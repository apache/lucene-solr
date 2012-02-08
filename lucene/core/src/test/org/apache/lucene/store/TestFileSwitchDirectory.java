package org.apache.lucene.store;

/**
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TestIndexWriterReader;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestFileSwitchDirectory extends LuceneTestCase {
  /**
   * Test if writing doc stores to disk and everything else to ram works.
   * @throws IOException
   */
  public void testBasic() throws IOException {
    Set<String> fileExtensions = new HashSet<String>();
    fileExtensions.add(IndexFileNames.FIELDS_EXTENSION);
    fileExtensions.add(IndexFileNames.FIELDS_INDEX_EXTENSION);
    
    MockDirectoryWrapper primaryDir = new MockDirectoryWrapper(random, new RAMDirectory());
    primaryDir.setCheckIndexOnClose(false); // only part of an index
    MockDirectoryWrapper secondaryDir = new MockDirectoryWrapper(random, new RAMDirectory());
    secondaryDir.setCheckIndexOnClose(false); // only part of an index
    
    FileSwitchDirectory fsd = new FileSwitchDirectory(fileExtensions, primaryDir, secondaryDir, true);
    IndexWriter writer = new IndexWriter(
        fsd,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMergePolicy(newLogMergePolicy(false))
    );
    TestIndexWriterReader.createIndexNoClose(true, "ram", writer);
    IndexReader reader = IndexReader.open(writer, true);
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
    Directory a = new SimpleFSDirectory(_TestUtil.getTempDir("foo"));
    Directory b = new SimpleFSDirectory(_TestUtil.getTempDir("bar"));
    FileSwitchDirectory switchDir = new FileSwitchDirectory(primaryExtensions, a, b, true);
    return new MockDirectoryWrapper(random, switchDir);
  }
  
  // LUCENE-3380 -- make sure we get exception if the directory really does not exist.
  public void testNoDir() throws Throwable {
    Directory dir = newFSSwitchDirectory(Collections.<String>emptySet());
    try {
      IndexReader.open(dir, true);
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
      dir.createOutput(name).close();
      assertTrue(dir.fileExists(name));
      assertTrue(Arrays.asList(dir.listAll()).contains(name));
    } finally {
      dir.close();
    }
  }
}
