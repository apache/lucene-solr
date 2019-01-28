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
package org.apache.solr.core;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

/**
 * test that configs can override the DirectoryFactory and 
 * IndexReaderFactory used in solr.
 */
public class AlternateDirectoryTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-altdirectory.xml", "schema.xml");
  }

  public void testAltDirectoryUsed() throws Exception {
    assertQ(req("q","*:*","qt","/select"));
    assertTrue(TestFSDirectoryFactory.openCalled);
    assertTrue(TestIndexReaderFactory.newReaderCalled);
  }
  
  public void testAltReaderUsed() throws Exception {
    IndexReaderFactory readerFactory = h.getCore().getIndexReaderFactory();
    assertNotNull("Factory is null", readerFactory);
    assertEquals("readerFactory is wrong class",
                 AlternateDirectoryTest.TestIndexReaderFactory.class.getName(), 
                 readerFactory.getClass().getName());
  }

  static public class TestFSDirectoryFactory extends StandardDirectoryFactory {
    public static volatile boolean openCalled = false;
    public static volatile Directory dir;
    
    @Override
    public Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
      openCalled = true;

      // we pass NoLockFactory, because the real lock factory is set later by injectLockFactory:
      return dir = newFSDirectory(new File(path).toPath(), lockFactory);
    }

  }


  static public class TestIndexReaderFactory extends IndexReaderFactory {
    static volatile boolean newReaderCalled = false;

    @Override
    public DirectoryReader newReader(Directory indexDir, SolrCore core) throws IOException {
      TestIndexReaderFactory.newReaderCalled = true;
      return DirectoryReader.open(indexDir);
    }

    @Override
    public DirectoryReader newReader(IndexWriter writer, SolrCore core) throws IOException {
      TestIndexReaderFactory.newReaderCalled = true;
      return DirectoryReader.open(writer);
    }
  }

}
