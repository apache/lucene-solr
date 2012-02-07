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

import java.util.Arrays;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

public class TestPagedBytes extends LuceneTestCase {

  public void testDataInputOutput() throws Exception {
    for(int iter=0;iter<5*RANDOM_MULTIPLIER;iter++) {
      final PagedBytes p = new PagedBytes(_TestUtil.nextInt(random, 1, 20));
      final DataOutput out = p.getDataOutput();
      final int numBytes = random.nextInt(10000000);

      final byte[] answer = new byte[numBytes];
      random.nextBytes(answer);
      int written = 0;
      while(written < numBytes) {
        if (random.nextInt(10) == 7) {
          out.writeByte(answer[written++]);
        } else {
          int chunk = Math.min(random.nextInt(1000), numBytes - written);
          out.writeBytes(answer, written, chunk);
          written += chunk;
        }
      }

      p.freeze(random.nextBoolean());

      final DataInput in = p.getDataInput();

      final byte[] verify = new byte[numBytes];
      int read = 0;
      while(read < numBytes) {
        if (random.nextInt(10) == 7) {
          verify[read++] = in.readByte();
        } else {
          int chunk = Math.min(random.nextInt(1000), numBytes - read);
          in.readBytes(verify, read, chunk);
          read += chunk;
        }
      }
      assertTrue(Arrays.equals(answer, verify));
    }
  }
}
