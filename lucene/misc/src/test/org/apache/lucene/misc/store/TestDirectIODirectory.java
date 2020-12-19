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

import static org.apache.lucene.misc.store.DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.BeforeClass;

public class TestDirectIODirectory extends BaseDirectoryTestCase {
  
  @BeforeClass
  public static void checkSupported() {
    assumeTrue("This test required a JDK version that has support for ExtendedOpenOption.DIRECT",
        DirectIODirectory.ExtendedOpenOption_DIRECT != null);
  }
  
  public void testWriteReadWithDirectIO() throws IOException {
    final Path path = createTempDir("testWriteReadWithDirectIO");
    final long blockSize = Files.getFileStore(path).getBlockSize();
    try(DirectIODirectory dir = getDirectory(path)) {
      final long minBytesDirect = Double.valueOf(Math.ceil(DEFAULT_MIN_BYTES_DIRECT / blockSize)).longValue() *
                                    blockSize;
      // Need to worry about overflows here?
      final int writtenByteLength = Math.toIntExact(minBytesDirect);

      MergeInfo mergeInfo = new MergeInfo(1000, Integer.MAX_VALUE, true, 1);
      final IOContext context = new IOContext(mergeInfo);

      IndexOutput indexOutput = dir.createOutput("test", context);
      indexOutput.writeBytes(new byte[writtenByteLength], 0, writtenByteLength);
      IndexInput indexInput = dir.openInput("test", context);

      assertEquals("The length of bytes read should equal to written", writtenByteLength, indexInput.length());

      indexOutput.close();
      indexInput.close();
    }
  }

  @Override
  protected DirectIODirectory getDirectory(Path path) throws IOException {
    Directory delegate = LuceneTestCase.newFSDirectory(path);
    return new DirectIODirectory(path, delegate);
  }
}