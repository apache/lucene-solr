package org.apache.lucene.util;

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

import java.util.Arrays;
import java.util.Random;

import org.apache.lucene.util.PagedBytes.PagedBytesDataInput;
import org.apache.lucene.util.PagedBytes.PagedBytesDataOutput;
import org.junit.Ignore;

@Ignore("You must increase heap to > 2 G to run this")
public class Test2BPagedBytes extends LuceneTestCase {

  public void test() throws Exception {
    PagedBytes pb = new PagedBytes(15);
    PagedBytesDataOutput dataOutput = pb.getDataOutput();
    long netBytes = 0;
    long seed = random().nextLong();
    long lastFP = 0;
    Random r2 = new Random(seed);
    while(netBytes < 1.1*Integer.MAX_VALUE) {
      int numBytes = _TestUtil.nextInt(r2, 1, 100000);
      byte[] bytes = new byte[numBytes];
      r2.nextBytes(bytes);
      dataOutput.writeBytes(bytes, bytes.length);
      long fp = dataOutput.getPosition();
      assert fp == lastFP + numBytes;
      lastFP = fp;
      netBytes += numBytes;
    }
    pb.freeze(true);

    PagedBytesDataInput dataInput = pb.getDataInput();
    lastFP = 0;
    r2 = new Random(seed);
    netBytes = 0;
    while(netBytes < 1.1*Integer.MAX_VALUE) {
      int numBytes = _TestUtil.nextInt(r2, 1, 100000);
      byte[] bytes = new byte[numBytes];
      r2.nextBytes(bytes);

      byte[] bytesIn = new byte[numBytes];
      dataInput.readBytes(bytesIn, 0, numBytes);
      assertTrue(Arrays.equals(bytes, bytesIn));

      long fp = dataInput.getPosition();
      assert fp == lastFP + numBytes;
      lastFP = fp;
      netBytes += numBytes;
    }
  }
}
