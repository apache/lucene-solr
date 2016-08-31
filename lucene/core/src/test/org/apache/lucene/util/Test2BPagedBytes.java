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
package org.apache.lucene.util;


import java.util.Random;

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase.Monster;

@Monster("You must increase heap to > 2 G to run this")
public class Test2BPagedBytes extends LuceneTestCase {

  public void test() throws Exception {
    BaseDirectoryWrapper dir = newFSDirectory(createTempDir("test2BPagedBytes"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    PagedBytes pb = new PagedBytes(15);
    IndexOutput dataOutput = dir.createOutput("foo", IOContext.DEFAULT);
    long netBytes = 0;
    long seed = random().nextLong();
    long lastFP = 0;
    Random r2 = new Random(seed);
    while(netBytes < 1.1*Integer.MAX_VALUE) {
      int numBytes = TestUtil.nextInt(r2, 1, 32768);
      byte[] bytes = new byte[numBytes];
      r2.nextBytes(bytes);
      dataOutput.writeBytes(bytes, bytes.length);
      long fp = dataOutput.getFilePointer();
      assert fp == lastFP + numBytes;
      lastFP = fp;
      netBytes += numBytes;
    }
    dataOutput.close();
    IndexInput input = dir.openInput("foo", IOContext.DEFAULT);
    pb.copy(input, input.length());
    input.close();
    PagedBytes.Reader reader = pb.freeze(true);

    r2 = new Random(seed);
    netBytes = 0;
    while(netBytes < 1.1*Integer.MAX_VALUE) {
      int numBytes = TestUtil.nextInt(r2, 1, 32768);
      byte[] bytes = new byte[numBytes];
      r2.nextBytes(bytes);
      BytesRef expected = new BytesRef(bytes);

      BytesRef actual = new BytesRef();
      reader.fillSlice(actual, netBytes, numBytes);
      assertEquals(expected, actual);

      netBytes += numBytes;
    }
    dir.close();
  }
}
