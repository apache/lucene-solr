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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.LuceneTestCase;
import java.io.IOException;

/**
 * Test-case for RAMDirectoryFactory
 */
public class RAMDirectoryFactoryTest extends LuceneTestCase {
  public void testOpenReturnsTheSameForSamePath() throws IOException {
    final Directory directory = new RefCntRamDirectory();
    RAMDirectoryFactory factory = new RAMDirectoryFactory()  {
      @Override
      Directory openNew(String path) throws IOException {
        return directory;
      }
    };
    String path = "/fake/path";
    Directory dir1 = factory.open(path);
    Directory dir2 = factory.open(path);
    assertEquals("RAMDirectoryFactory should not create new instance of RefCntRamDirectory " +
        "every time open() is called for the same path", directory, dir1);
    assertEquals("RAMDirectoryFactory should not create new instance of RefCntRamDirectory " +
        "every time open() is called for the same path", directory, dir2);
    dir1.close();
    dir2.close();
  }

  public void testOpenSucceedForEmptyDir() throws IOException {
    RAMDirectoryFactory factory = new RAMDirectoryFactory();
    Directory dir = factory.open("/fake/path");
    assertNotNull("RAMDirectoryFactory should create RefCntRamDirectory even if the path doen't lead " +
        "to index directory on the file system", dir);
  }
}
