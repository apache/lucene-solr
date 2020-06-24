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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.store.EncryptingIndexInput.HEADER_IV_LENGTH;
import static org.junit.Assert.*;
import static org.apache.lucene.store.EncryptingUtil.IV_LENGTH;

public class TestEncryptingIndexOutput extends BaseDataOutputTestCase<EncryptingIndexOutput> {

  private byte[] key;
  private boolean shouldSimulateWrongKey;

  @Before
  public void initializeEncryption() {
    key = randomBytesOfLength(32);
    shouldSimulateWrongKey = false;
  }

  @Test
  public void testEncryptionLength() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamIndexOutput delegateIndexOutput = new OutputStreamIndexOutput("test", "test", baos, 10);
    byte[] key = new byte[32];
    Arrays.fill(key, (byte) 1);
    EncryptingIndexOutput indexOutput = new EncryptingIndexOutput(delegateIndexOutput, key) {
      @Override
      protected int getBufferCapacity() {
        return EncryptingUtil.AES_BLOCK_SIZE;
      }
    };
    indexOutput.writeByte((byte) 3);
    assertEquals(1, indexOutput.getFilePointer());
    byte[] bytes = "tomorrow morning".getBytes(StandardCharsets.UTF_16);
    indexOutput.writeBytes(bytes, 0, bytes.length);
    assertEquals(1 + bytes.length, indexOutput.getFilePointer());
    indexOutput.close();
    assertEquals(1 + bytes.length + HEADER_IV_LENGTH + CodecUtil.footerLength(), baos.size());
  }

  @Test
  @Override
  public void testRandomizedWrites() throws IOException {
    // First check encryption and decryption.
    super.testRandomizedWrites();

    // Then modify the secret key and check it fails.
    shouldSimulateWrongKey = true;
    LuceneTestCase.expectThrows(AssertionError.class, super::testRandomizedWrites);
  }

  @Override
  protected EncryptingIndexOutput newInstance() {
    try {
      return new MyBufferedEncryptingIndexOutput(new ByteBuffersDataOutput(), key) {
        @Override
        protected byte[] generateRandomIv() {
          byte[] iv = new byte[IV_LENGTH];
          getRandom().nextBytes(iv);
          return iv;
        }
      };
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected byte[] toBytes(EncryptingIndexOutput indexOutput) {
    try {
      indexOutput.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    ByteBuffersDataInput dataInput = ((MyBufferedEncryptingIndexOutput) indexOutput).dataOutput.toDataInput();
    IndexInput indexInput = new ByteBuffersIndexInput(dataInput, "Test");
    byte[] key = this.key.clone();
    if (shouldSimulateWrongKey) {
      key[0]++;
    }
    try (EncryptingIndexInput encryptingIndexInput = new EncryptingIndexInput(indexInput, key)) {
      byte[] b = new byte[(int) encryptingIndexInput.length()];
      encryptingIndexInput.readBytes(b, 0, b.length);
      return b;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Replaces the {@link java.security.SecureRandom} by a repeatable {@link Random} for tests.
   * This is used to generate a repeatable random IV.
   */
  private static class MyBufferedEncryptingIndexOutput extends EncryptingIndexOutput {

    private final ByteBuffersDataOutput dataOutput;

    MyBufferedEncryptingIndexOutput(ByteBuffersDataOutput dataOutput, byte[] key) throws IOException {
      super(new ByteBuffersIndexOutput(dataOutput, "Test", "Test"), key);
      this.dataOutput = dataOutput;
    }
  }
}