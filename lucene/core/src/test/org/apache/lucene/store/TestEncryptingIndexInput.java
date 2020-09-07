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

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.Xoroshiro128PlusRandom;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.crypto.AesCtrEncrypterFactory;
import org.apache.lucene.util.crypto.CipherAesCtrEncrypter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestEncryptingIndexInput extends RandomizedTest {

  private byte[] key;

  @Before
  public void initializeEncryption() {
    // AES key length can either 16, 24 or 32 bytes.
    key = randomBytesOfLength(randomIntBetween(2, 4) * 8);
  }

  @Test
  public void testSanity() throws IOException {
    ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
    EncryptingIndexOutput indexOutput = createEncryptingIndexOutput(dataOutput);
    indexOutput.close();
    EncryptingIndexInput indexInput1 = createEncryptingIndexInput(dataOutput);
    assertEquals(0, indexInput1.length());
    LuceneTestCase.expectThrows(EOFException.class, indexInput1::readByte);

    dataOutput = new ByteBuffersDataOutput();
    indexOutput = createEncryptingIndexOutput(dataOutput);
    indexOutput.writeByte((byte) 1);
    indexOutput.close();

    EncryptingIndexInput indexInput2 = createEncryptingIndexInput(dataOutput);
    assertEquals(1, indexInput2.length());
    assertEquals(0, indexInput2.getFilePointer());
    assertEquals(0, indexInput1.length());
    indexInput1.close();

    assertEquals(1, indexInput2.readByte());
    assertEquals(1, indexInput2.getFilePointer());
    assertEquals(1, indexInput2.randomAccessSlice(0, 1).readByte(0));

    LuceneTestCase.expectThrows(EOFException.class, indexInput2::readByte);

    assertEquals(1, indexInput2.getFilePointer());
    indexInput2.close();
  }

  @Test
  public void testRandomReads() throws Exception {
    ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
    EncryptingIndexOutput indexOutput = createEncryptingIndexOutput(dataOutput);

    long seed = randomLong();
    int max = LuceneTestCase.TEST_NIGHTLY ? 1_000_000 : 100_000;
    List<IOUtils.IOConsumer<DataInput>> reply =
        TestByteBuffersDataOutput.addRandomData(indexOutput, new Xoroshiro128PlusRandom(seed), max);
    indexOutput.close();

    EncryptingIndexInput indexInput = createEncryptingIndexInput(dataOutput);
    for (IOUtils.IOConsumer<DataInput> c : reply) {
      c.accept(indexInput);
    }

    LuceneTestCase.expectThrows(EOFException.class, indexInput::readByte);
    indexInput.close();
  }

  @Test
  public void testRandomReadsOnSlices() throws Exception {
    for (int reps = randomIntBetween(1, 20); --reps > 0; ) {
      ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
      EncryptingIndexOutput indexOutput = createEncryptingIndexOutput(dataOutput);

      byte[] prefix = new byte[randomIntBetween(0, 1024 * 8)];
      indexOutput.writeBytes(prefix, 0, prefix.length);

      long seed = randomLong();
      int max = 10_000;
      List<IOUtils.IOConsumer<DataInput>> reply =
          TestByteBuffersDataOutput.addRandomData(indexOutput, new Xoroshiro128PlusRandom(seed), max);

      byte[] suffix = new byte[randomIntBetween(0, 1024 * 8)];
      indexOutput.writeBytes(suffix, 0, suffix.length);
      long outputLength = indexOutput.getFilePointer();
      indexOutput.close();

      IndexInput indexInput = createEncryptingIndexInput(dataOutput).slice("Test", prefix.length, outputLength - prefix.length - suffix.length);

      assertEquals(0, indexInput.getFilePointer());
      assertEquals(outputLength - prefix.length - suffix.length, indexInput.length());
      for (IOUtils.IOConsumer<DataInput> c : reply) {
        c.accept(indexInput);
      }

      LuceneTestCase.expectThrows(EOFException.class, indexInput::readByte);
      indexInput.close();
    }
  }

  @Test
  public void testSeekEmpty() throws Exception {
    ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
    createEncryptingIndexOutput(dataOutput).close();
    EncryptingIndexInput indexInput = createEncryptingIndexInput(dataOutput);

    indexInput.seek(0);
    LuceneTestCase.expectThrows(EOFException.class, () -> indexInput.seek(1));

    indexInput.seek(0);
    LuceneTestCase.expectThrows(EOFException.class, indexInput::readByte);
    indexInput.close();
  }

  @Test
  public void testSeek() throws Exception {
    for (int reps = randomIntBetween(1, 200); --reps > 0; ) {
      ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
      ByteBuffersDataOutput clearDataOutput = new ByteBuffersDataOutput();
      EncryptingIndexOutput indexOutput = createEncryptingIndexOutput(dataOutput);

      byte[] prefix = {};
      if (randomBoolean()) {
        prefix = new byte[randomIntBetween(1, 1024 * 8)];
        indexOutput.writeBytes(prefix, 0, prefix.length);
        clearDataOutput.writeBytes(prefix);
      }

      long seed = randomLong();
      int max = 1000;
      List<IOUtils.IOConsumer<DataInput>> reply =
          TestByteBuffersDataOutput.addRandomData(indexOutput, new Xoroshiro128PlusRandom(seed), max);
      TestByteBuffersDataOutput.addRandomData(clearDataOutput, new Xoroshiro128PlusRandom(seed), max);
      assertEquals(clearDataOutput.size(), indexOutput.getFilePointer());
      long outputLength = indexOutput.getFilePointer();
      indexOutput.close();

      IndexInput indexInput = createEncryptingIndexInput(dataOutput).slice("Test", prefix.length, outputLength - prefix.length);

      indexInput.seek(0);
      for (IOUtils.IOConsumer<DataInput> c : reply) {
        c.accept(indexInput);
      }

      indexInput.seek(0);
      for (IOUtils.IOConsumer<DataInput> c : reply) {
        c.accept(indexInput);
      }

      byte[] clearData = clearDataOutput.toArrayCopy();
      clearData = ArrayUtil.copyOfSubArray(clearData, prefix.length, clearData.length);

      for (int i = 0; i < 1000; i++) {
        int offs = randomIntBetween(0, clearData.length - 1);
        indexInput.seek(offs);
        assertEquals(offs, indexInput.getFilePointer());
        assertEquals("reps=" + reps + " i=" + i + ", offs=" + offs, clearData[offs], indexInput.readByte());
      }
      indexInput.seek(indexInput.length());
      assertEquals(indexInput.length(), indexInput.getFilePointer());
      LuceneTestCase.expectThrows(EOFException.class, indexInput::readByte);
      indexInput.close();
    }
  }

  @Test
  public void testClone() throws Exception {
    for (int reps = randomIntBetween(1, 200); --reps > 0; ) {
      ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
      ByteBuffersDataOutput clearDataOutput = new ByteBuffersDataOutput();
      EncryptingIndexOutput indexOutput = createEncryptingIndexOutput(dataOutput);

      long seed = randomLong();
      int max = 1000;
      TestByteBuffersDataOutput.addRandomData(indexOutput, new Xoroshiro128PlusRandom(seed), max);
      TestByteBuffersDataOutput.addRandomData(clearDataOutput, new Xoroshiro128PlusRandom(seed), max);
      assertEquals(clearDataOutput.size(), indexOutput.getFilePointer());
      indexOutput.close();

      IndexInput indexInput = createEncryptingIndexInput(dataOutput);
      byte[] clearData = clearDataOutput.toArrayCopy();
      byte[] readBuffer = new byte[100];
      for (int i = 0; i < 1000; i++) {
        int readLength = randomIntBetween(1, readBuffer.length);
        int offs = randomIntBetween(0, clearData.length - 1 - 2 * readLength);
        indexInput.seek(offs);
        assertEquals("reps=" + reps + " i=" + i + ", offs=" + offs, clearData[offs], indexInput.readByte());
        indexInput.readBytes(readBuffer, 0, readLength);
        assertTrue(Arrays.equals(clearData, offs + 1, offs + 1 + readLength, readBuffer, 0, readLength));

        IndexInput clone = indexInput.clone();
        if (randomBoolean()) {
          clone.readBytes(readBuffer, 0, readLength);
          assertTrue(Arrays.equals(clearData, offs + 1 + readLength, offs + 1 + 2 * readLength, readBuffer, 0, readLength));
        }
        int cloneOffs = Math.max(offs - readLength + randomIntBetween(0, 2 * readLength), 0);
        clone.seek(cloneOffs);
        clone.readBytes(readBuffer, 0, readLength);
        assertTrue(Arrays.equals(clearData, cloneOffs, cloneOffs + readLength, readBuffer, 0, readLength));
        clone.close();
      }
      indexInput.close();
    }
  }

  private EncryptingIndexOutput createEncryptingIndexOutput(ByteBuffersDataOutput dataOutput) throws IOException {
    return new EncryptingIndexOutput(new ByteBuffersIndexOutput(dataOutput, "Test", "Test"),
        key, null, encrypterFactory());
  }

  private EncryptingIndexInput createEncryptingIndexInput(ByteBuffersDataOutput dataOutput) throws IOException {
    return new EncryptingIndexInput(new ByteBuffersIndexInput(dataOutput.toDataInput(), "Test"),
        key, encrypterFactory());
  }

  protected AesCtrEncrypterFactory encrypterFactory() {
    return CipherAesCtrEncrypter.FACTORY;
  }
}
