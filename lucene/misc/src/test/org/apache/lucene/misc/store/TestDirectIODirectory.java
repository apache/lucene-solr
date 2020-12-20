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
package org.apache.lucene.misc.store;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.junit.BeforeClass;

public class TestDirectIODirectory extends BaseDirectoryTestCase {
  
  @BeforeClass
  public static void checkSupported() {
    assumeTrue("This test required a JDK version that has support for ExtendedOpenOption.DIRECT",
        DirectIODirectory.ExtendedOpenOption_DIRECT != null);
  }
  
  @Override
  protected DirectIODirectory getDirectory(Path path) throws IOException {
    return new DirectIODirectory(FSDirectory.open(path), DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE, 0L) {
      @Override
      protected boolean useDirectIO(IOContext context) {
        return true;
      }
    };
  }
}