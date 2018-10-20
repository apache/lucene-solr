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
package org.apache.lucene.store;


import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

/** Simple tests for SingleInstanceLockFactory */
public class TestSingleInstanceLockFactory extends BaseLockFactoryTestCase {
  
  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return newDirectory(random(), new SingleInstanceLockFactory());
  }
  
  // Verify: SingleInstanceLockFactory is the default lock for RAMDirectory
  // Verify: RAMDirectory does basic locking correctly (can't create two IndexWriters)
  public void testDefaultRAMDirectory() throws IOException {
    RAMDirectory dir = new RAMDirectory();
    
    assertTrue("RAMDirectory did not use correct LockFactory: got " + dir.lockFactory,
        dir.lockFactory instanceof SingleInstanceLockFactory);
    
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));
    
    // Create a 2nd IndexWriter.  This should fail:
    expectThrows(IOException.class, () -> {
      new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
    });
    
    writer.close();
  }
}
