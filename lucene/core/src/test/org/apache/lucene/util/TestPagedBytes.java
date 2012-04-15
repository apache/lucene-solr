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

package org.apache.lucene.util;

import java.util.*;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

public class TestPagedBytes extends LuceneTestCase {

  public void testDataInputOutput() throws Exception {
    Random random = random();
    for(int iter=0;iter<5*RANDOM_MULTIPLIER;iter++) {
      final int blockBits = _TestUtil.nextInt(random, 1, 20);
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

  public void testLengthPrefix() throws Exception {
    Random random = random();
    for(int iter=0;iter<5*RANDOM_MULTIPLIER;iter++) {
      final int blockBits = _TestUtil.nextInt(random, 2, 20);
      final int blockSize = 1 << blockBits;
      final PagedBytes p = new PagedBytes(blockBits);
      final List<Integer> addresses = new ArrayList<Integer>();
      final List<BytesRef> answers = new ArrayList<BytesRef>();
      int totBytes = 0;
      while(totBytes < 10000000 && answers.size() < 100000) {
        final int len = random.nextInt(Math.min(blockSize-2, 32768));
        final BytesRef b = new BytesRef();
        b.bytes = new byte[len];
        b.length = len;
        b.offset = 0;
        random.nextBytes(b.bytes);
        answers.add(b);
        addresses.add((int) p.copyUsingLengthPrefix(b));

        totBytes += len;
      }

      final PagedBytes.Reader reader = p.freeze(random.nextBoolean());

      final BytesRef slice = new BytesRef();

      for(int idx=0;idx<answers.size();idx++) {
        reader.fillSliceWithPrefix(slice, addresses.get(idx));
        assertEquals(answers.get(idx), slice);
      }
    }
  }

  // LUCENE-3841: even though
  // copyUsingLengthPrefix will never span two blocks, make
  // sure if caller writes their own prefix followed by the
  // bytes, it still works:
  public void testLengthPrefixAcrossTwoBlocks() throws Exception {
    Random random = random();
    final PagedBytes p = new PagedBytes(10);
    final DataOutput out = p.getDataOutput();
    final byte[] bytes1 = new byte[1000];
    random.nextBytes(bytes1);
    out.writeBytes(bytes1, 0, bytes1.length);
    out.writeByte((byte) 40);
    final byte[] bytes2 = new byte[40];
    random.nextBytes(bytes2);
    out.writeBytes(bytes2, 0, bytes2.length);

    final PagedBytes.Reader reader = p.freeze(random.nextBoolean());
    BytesRef answer = reader.fillSliceWithPrefix(new BytesRef(), 1000);
    assertEquals(40, answer.length);
    for(int i=0;i<40;i++) {
      assertEquals(bytes2[i], answer.bytes[answer.offset + i]);
    }
  }
}
