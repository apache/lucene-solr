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
package org.apache.lucene.index;


import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.junit.Test;

public class TestCheckIndex extends BaseTestCheckIndex {
  private Directory directory;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
  }

  @Override
  public void tearDown() throws Exception {
    directory.close();
    super.tearDown();
  }
  
  @Test
  public void testDeletedDocs() throws IOException {
    testDeletedDocs(directory);
  }
  
  @Test
  public void testBogusTermVectors() throws IOException {
    testBogusTermVectors(directory);
  }
  
  @Test
  public void testChecksumsOnly() throws IOException {
    testChecksumsOnly(directory);
  }
  
  @Test
  public void testChecksumsOnlyVerbose() throws IOException {
    testChecksumsOnlyVerbose(directory);
  }

  @Test
  public void testObtainsLock() throws IOException {
    testObtainsLock(directory);
  }
}
