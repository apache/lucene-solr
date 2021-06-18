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

import java.io.IOException;
import java.util.*;

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.junit.Ignore;

public class TestPagedBytes extends LuceneTestCase {

  // Writes random byte/s to "normal" file in dir, then
  // copies into PagedBytes and verifies with
  // PagedBytes.Reader: 
  public void testDataInputOutput() throws Exception {
    Random random = random();
    int numIters = atLeast(1);
    for(int iter=0;iter<numIters;iter++) {
      BaseDirectoryWrapper dir = newFSDirectory(createTempDir("testOverflow"));
      if (dir instanceof MockDirectoryWrapper) {
        ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
      }
      final int blockBits = TestUtil.nextInt(random, 1, 20);
      final int blockSize = 1 << blockBits;
      final PagedBytes p = new PagedBytes(blockBits);
      final IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT);
      final int numBytes = TestUtil.nextInt(random, 2, 10000000);

      final byte[] answer = new byte[numBytes];
      random.nextBytes(answer);
      int written = 0;
      while(written < numBytes) {
        if (random.nextInt(100) == 7) {
          out.writeByte(answer[written++]);
        } else {
          int chunk = Math.min(random.nextInt(1000), numBytes - written);
          out.writeBytes(answer, written, chunk);
          written += chunk;
        }
      }
      
      out.close();
      final IndexInput input = dir.openInput("foo", IOContext.DEFAULT);
      final DataInput in = input.clone();
      
      p.copy(input, input.length());
      final PagedBytes.Reader reader = p.freeze(random.nextBoolean());

      final byte[] verify = new byte[numBytes];
      int read = 0;
      while(read < numBytes) {
        if (random.nextInt(100) == 7) {
          verify[read++] = in.readByte();
        } else {
          int chunk = Math.min(random.nextInt(1000), numBytes - read);
          in.readBytes(verify, read, chunk);
          read += chunk;
        }
      }
      assertTrue(Arrays.equals(answer, verify));

      final BytesRef slice = new BytesRef();
      for(int iter2=0;iter2<100;iter2++) {
        final int pos = random.nextInt(numBytes-1);
        assertEquals(answer[pos], reader.getByte(pos));
        final int len = random.nextInt(Math.min(blockSize+1, numBytes - pos));
        reader.fillSlice(slice, pos, len);
        for(int byteUpto=0;byteUpto<len;byteUpto++) {
          assertEquals(answer[pos + byteUpto], slice.bytes[slice.offset + byteUpto]);
        }
      }
      input.close();
      dir.close();
    }
  }

  // Writes random byte/s into PagedBytes via
  // .getDataOutput(), then verifies with
  // PagedBytes.getDataInput(): 
  public void testDataInputOutput2() throws Exception {
    Random random = random();
    int numIters = atLeast(1);
    for(int iter=0;iter<numIters;iter++) {
      final int blockBits = TestUtil.nextInt(random, 1, 20);
      final int blockSize = 1 << blockBits;
      final PagedBytes p = new PagedBytes(blockBits);
      final DataOutput out = p.getDataOutput();
      final int numBytes = random().nextInt(10000000);

      final byte[] answer = new byte[numBytes];
      random().nextBytes(answer);
      int written = 0;
      while(written < numBytes) {
        if (random().nextInt(10) == 7) {
          out.writeByte(answer[written++]);
        } else {
          int chunk = Math.min(random().nextInt(1000), numBytes - written);
          out.writeBytes(answer, written, chunk);
          written += chunk;
        }
      }

      final PagedBytes.Reader reader = p.freeze(random.nextBoolean());

      final DataInput in = p.getDataInput();

      final byte[] verify = new byte[numBytes];
      int read = 0;
      while(read < numBytes) {
        if (random().nextInt(10) == 7) {
          verify[read++] = in.readByte();
        } else {
          int chunk = Math.min(random().nextInt(1000), numBytes - read);
          in.readBytes(verify, read, chunk);
          read += chunk;
        }
      }
      assertTrue(Arrays.equals(answer, verify));

      final BytesRef slice = new BytesRef();
      for(int iter2=0;iter2<100;iter2++) {
        final int pos = random.nextInt(numBytes-1);
        final int len = random.nextInt(Math.min(blockSize+1, numBytes - pos));
        reader.fillSlice(slice, pos, len);
        for(int byteUpto=0;byteUpto<len;byteUpto++) {
          assertEquals(answer[pos + byteUpto], slice.bytes[slice.offset + byteUpto]);
        }
      }
    }
  }

  @Ignore // memory hole
  public void testOverflow() throws IOException {
    BaseDirectoryWrapper dir = newFSDirectory(createTempDir("testOverflow"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    final int blockBits = TestUtil.nextInt(random(), 14, 28);
    final int blockSize = 1 << blockBits;
    byte[] arr = new byte[TestUtil.nextInt(random(), blockSize / 2, blockSize * 2)];
    for (int i = 0; i < arr.length; ++i) {
      arr[i] = (byte) i;
    }
    final long numBytes = (1L << 31) + TestUtil.nextInt(random(), 1, blockSize * 3);
    final PagedBytes p = new PagedBytes(blockBits);
    final IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT);
    for (long i = 0; i < numBytes; ) {
      assertEquals(i, out.getFilePointer());
      final int len = (int) Math.min(arr.length, numBytes - i);
      out.writeBytes(arr, len);
      i += len;
    }
    assertEquals(numBytes, out.getFilePointer());
    out.close();
    final IndexInput in = dir.openInput("foo", IOContext.DEFAULT);
    p.copy(in, numBytes);
    final PagedBytes.Reader reader = p.freeze(random().nextBoolean());

    for (long offset : new long[] {0L, Integer.MAX_VALUE, numBytes - 1,
        TestUtil.nextLong(random(), 1, numBytes - 2)}) {
      BytesRef b = new BytesRef();
      reader.fillSlice(b, offset, 1);
      assertEquals(arr[(int) (offset % arr.length)], b.bytes[b.offset]);
    }
    in.close();
    dir.close();
  }

  public void testRamBytesUsed() {
    final int blockBits = TestUtil.nextInt(random(), 4, 22);
    PagedBytes b = new PagedBytes(blockBits);
    final int totalBytes = random().nextInt(10000);
    for (long pointer = 0; pointer < totalBytes; ) {
      BytesRef bytes = new BytesRef(TestUtil.randomSimpleString(random(), 10));
      pointer = b.copyUsingLengthPrefix(bytes);
    }
    assertEquals(RamUsageTester.sizeOf(b), b.ramBytesUsed());
    final PagedBytes.Reader reader = b.freeze(random().nextBoolean());
    assertEquals(RamUsageTester.sizeOf(b), b.ramBytesUsed());
    assertEquals(RamUsageTester.sizeOf(reader), reader.ramBytesUsed());
  }

}
