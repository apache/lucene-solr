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
package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils.IOConsumer;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.Xoroshiro128PlusRandom;
import com.carrotsearch.randomizedtesting.generators.RandomBytes;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

public abstract class BaseDataOutputTestCase<T extends DataOutput> extends RandomizedTest {
  protected abstract T newInstance();
  protected abstract byte[] toBytes(T instance);
  
  @FunctionalInterface
  private interface ThrowingBiFunction<T, U, R> {
    R apply(T t, U u) throws Exception;
  }

  @Test
  public void testRandomizedWrites() throws IOException {
    T dst = newInstance();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput ref = new OutputStreamDataOutput(baos);

    long seed = randomLong();
    int max = 50_000;
    addRandomData(dst, new Xoroshiro128PlusRandom(seed), max);
    addRandomData(ref, new Xoroshiro128PlusRandom(seed), max);
    assertArrayEquals(baos.toByteArray(), toBytes(dst));
  }
  
  protected static List<IOConsumer<DataInput>> addRandomData(DataOutput dst, Random rnd, int maxAddCalls) throws IOException {
    try {
      List<IOConsumer<DataInput>> reply = new ArrayList<>();
      for (int i = 0; i < maxAddCalls; i++) {
        reply.add(RandomPicks.randomFrom(rnd, GENERATORS).apply(dst, rnd));
      }
      return reply;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }  

  private static List<ThrowingBiFunction<DataOutput, Random, IOConsumer<DataInput>>> GENERATORS;
  static {
    GENERATORS = new ArrayList<>();

    // writeByte/ readByte
    GENERATORS.add((dst, rnd) -> {
        byte value = (byte) rnd.nextInt();
        dst.writeByte(value);
        return (src) -> assertEquals("readByte()", value, src.readByte());
      });

    // writeBytes/ readBytes (array and buffer version).
    GENERATORS.add((dst, rnd) -> {
        byte[] bytes = RandomBytes.randomBytesOfLengthBetween(rnd, 0, 100);
        ByteBuffersDataOutput rdo = dst instanceof ByteBuffersDataOutput ? (ByteBuffersDataOutput) dst : null;

        if (rnd.nextBoolean() && rdo != null) {
          rdo.writeBytes(ByteBuffer.wrap(bytes));
        } else {
          dst.writeBytes(bytes, bytes.length);
        }

        boolean useBuffersForRead = rnd.nextBoolean();
        return (src) -> {
          byte [] read = new byte [bytes.length];
          if (useBuffersForRead && src instanceof ByteBuffersDataInput) {
            ((ByteBuffersDataInput) src).readBytes(ByteBuffer.wrap(read), read.length);
            assertArrayEquals("readBytes(ByteBuffer)", bytes, read);
          } else {
            src.readBytes(read, 0, read.length);
            assertArrayEquals("readBytes(byte[])", bytes, read);
          }
        };
      }
    );

    // writeBytes/ readBytes (array + offset).
    GENERATORS.add((dst, rnd) -> {
        byte[] bytes = RandomBytes.randomBytesOfLengthBetween(rnd, 0, 100);
        int off = RandomNumbers.randomIntBetween(rnd, 0, bytes.length);
        int len = RandomNumbers.randomIntBetween(rnd, 0, bytes.length - off); 
        dst.writeBytes(bytes, off, len);

        return (src) -> {
          byte [] read = new byte [bytes.length + off];
          src.readBytes(read, off, len);
          assertArrayEquals(
              "readBytes(byte[], off)",
              ArrayUtil.copyOfSubArray(bytes, off, len + off),
              ArrayUtil.copyOfSubArray(read, off, len + off));
        };
      }
    );
    
    GENERATORS.add((dst, rnd) -> {
      int v = rnd.nextInt(); 
      dst.writeInt(v); 
      return (src) -> assertEquals("readInt()", v, src.readInt());
    });

    GENERATORS.add((dst, rnd) -> {
      long v = rnd.nextLong(); 
      dst.writeLong(v); 
      return (src) -> assertEquals("readLong()", v, src.readLong());
    });
    
    GENERATORS.add((dst, rnd) -> {
      short v = (short) rnd.nextInt(); 
      dst.writeShort(v); 
      return (src) -> assertEquals("readShort()", v, src.readShort());
    });

    GENERATORS.add((dst, rnd) -> {
      int v = rnd.nextInt(); 
      dst.writeVInt(v); 
      return (src) -> assertEquals("readVInt()", v, src.readVInt());
    });

    GENERATORS.add((dst, rnd) -> {
      int v = rnd.nextInt(); 
      dst.writeZInt(v); 
      return (src) -> assertEquals("readZInt()", v, src.readZInt());
    });

    GENERATORS.add((dst, rnd) -> {
      long v = rnd.nextLong() & (-1L >>> 1); 
      dst.writeVLong(v); 
      return (src) -> assertEquals("readVLong()", v, src.readVLong());
    });

    GENERATORS.add((dst, rnd) -> {
      long v = rnd.nextLong(); 
      dst.writeZLong(v); 
      return (src) -> assertEquals("readZLong()", v, src.readZLong());
    });

    GENERATORS.add((dst, rnd) -> {
      String v;
      if (rnd.nextInt(50) == 0) {
        // Occasionally a large blob.
        v = RandomStrings.randomUnicodeOfLength(rnd, RandomNumbers.randomIntBetween(rnd, 2048, 4096));
      } else {
        v = RandomStrings.randomUnicodeOfLength(rnd, RandomNumbers.randomIntBetween(rnd, 0, 10));
      }
      dst.writeString(v);
      return (src) -> assertEquals("readString()", v, src.readString());
    });
  }
}
