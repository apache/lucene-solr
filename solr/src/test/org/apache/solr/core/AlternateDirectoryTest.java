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
package org.apache.solr.core;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class AlternateDirectoryTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-altdirectory.xml", "schema.xml");
  }

  /**
   * Simple test to ensure that alternate IndexReaderFactory is being used.
   * 
   * @throws Exception
   */
  @Test
  public void testAltDirectoryUsed() throws Exception {
    assertQ(req("q","*:*","qt","standard"));
    assertTrue(TestFSDirectoryFactory.openCalled);
    assertTrue(TestIndexReaderFactory.newReaderCalled);
    TestFSDirectoryFactory.dir.close();
  }

  static public class TestFSDirectoryFactory extends DirectoryFactory {
    public static volatile boolean openCalled = false;
    public static volatile Directory dir;
    
    @Override
    public Directory open(String path) throws IOException {
      openCalled = true;
      // need to close the directory, or otherwise the test fails.
      if (dir != null) {
        dir.close();
      }
      return dir = newFSDirectory(new File(path));
    }

  }


  static public class TestIndexReaderFactory extends IndexReaderFactory {
    static volatile boolean newReaderCalled = false;

    @Override
    public IndexReader newReader(Directory indexDir, boolean readOnly)
        throws IOException {
      TestIndexReaderFactory.newReaderCalled = true;
      return IndexReader.open(indexDir, readOnly);
    }
  }

}
