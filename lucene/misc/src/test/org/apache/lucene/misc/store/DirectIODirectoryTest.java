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

import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.lucene.store.*;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.nio.file.Files;

import static org.apache.lucene.misc.store.DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT;

public class DirectIODirectoryTest extends LuceneTestCase {

  public void testWriteReadMergeInfo() throws IOException {
    try (ByteBuffersDirectory ramDir = new ByteBuffersDirectory();
         Directory dir = new DirectIODirectory(RandomizedTest.newTempDir(LifecycleScope.TEST), ramDir)) {

      final long blockSize = Files.getFileStore(createTempFile()).getBlockSize();
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
}