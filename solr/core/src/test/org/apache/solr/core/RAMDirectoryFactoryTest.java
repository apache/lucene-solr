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

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.SolrTestCase;
import org.apache.solr.core.DirectoryFactory.DirContext;

/**
 * Test-case for RAMDirectoryFactory
 */
public class RAMDirectoryFactoryTest extends SolrTestCase {

  public void test() throws Exception {
    dotestOpenReturnsTheSameForSamePath();
    dotestOpenSucceedForEmptyDir();
  }

  private void dotestOpenReturnsTheSameForSamePath() throws IOException {
    final Directory directory = new RAMDirectory();
    RAMDirectoryFactory factory = new RAMDirectoryFactory()  {
      @Override
      protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) {
        return directory;
      }
    };
    String path = "/fake/path";
    Directory dir1 = factory.get(path, DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
    Directory dir2 = factory.get(path, DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
    assertEquals("RAMDirectoryFactory should not create new instance of RefCntRamDirectory " +
        "every time open() is called for the same path", dir1, dir2);

    factory.release(dir1);
    factory.release(dir2);
    factory.close();
  }

  private void dotestOpenSucceedForEmptyDir() throws IOException {
    RAMDirectoryFactory factory = new RAMDirectoryFactory();
    Directory dir = factory.get("/fake/path", DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
    assertNotNull("RAMDirectoryFactory should create RefCntRamDirectory even if the path doen't lead " +
        "to index directory on the file system", dir);
    factory.release(dir);
    factory.close();
  }
}
