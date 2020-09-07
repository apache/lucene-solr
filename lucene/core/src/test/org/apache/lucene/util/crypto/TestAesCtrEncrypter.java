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

package org.apache.lucene.util.crypto;

import java.nio.ByteBuffer;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

/**
 * Tests {@link AesCtrEncrypter} implementations.
 * For more random tests on {@link AesCtrEncrypter}, see {@link org.apache.lucene.store.TestEncryptingIndexOutput}
 * and {@link org.apache.lucene.store.TestEncryptingIndexInput}.
 */
public class TestAesCtrEncrypter extends LuceneTestCase {

  /**
   * Verifies that {@link AesCtrEncrypter} implementations encrypt and decrypt data exactly the
   * same way. They produce the same encrypted data and can decrypt each other.
   */
  @Test
  public void testEncryptionDecryption() {
    for (int i = 0; i < 100; i++) {
      ByteBuffer clearData = generateRandomData(10000);
      byte[] key = generateRandomBytes(EncryptionUtil.AES_BLOCK_SIZE);
      byte[] iv = EncryptionUtil.generateRandomAesCtrIv();
      AesCtrEncrypter lightEncrypter = encrypterFactory().create(key, iv);
      AesCtrEncrypter cipherEncrypter = encrypterFactory().create(key, iv);

      ByteBuffer encryptedDataLight = crypt(clearData, lightEncrypter);
      ByteBuffer encryptedDataCipher = crypt(clearData, cipherEncrypter);
      assertEquals(encryptedDataCipher, encryptedDataLight);

      ByteBuffer decryptedData = crypt(encryptedDataLight, lightEncrypter);
      assertEquals(clearData, decryptedData);
      decryptedData = crypt(encryptedDataLight, cipherEncrypter);
      assertEquals(clearData, decryptedData);
    }
  }

  protected AesCtrEncrypterFactory encrypterFactory() {
    return CipherAesCtrEncrypter.FACTORY;
  }

  private static ByteBuffer generateRandomData(int numBytes) {
    ByteBuffer buffer = ByteBuffer.allocate(numBytes);
    for (int i = 0; i < numBytes; i++) {
      buffer.put((byte) random().nextInt());
    }
    buffer.position(0);
    return buffer;
  }

  private static byte[] generateRandomBytes(int numBytes) {
    byte[] b = new byte[numBytes];
    // Strangely Random.nextBytes(byte[]) does not produce good enough randomness here.
    // It has a bias to produce 0 and -1 bytes. I don't understand why.
    for (int i = 0; i < numBytes; i++) {
      b[i] = (byte) random().nextInt();
    }
    return b;
  }

  private ByteBuffer crypt(ByteBuffer inputBuffer, AesCtrEncrypter encrypter) {
    encrypter = randomClone(encrypter);
    encrypter.init(0);
    int inputInitialPosition = inputBuffer.position();
    ByteBuffer outputBuffer = ByteBuffer.allocate(inputBuffer.capacity());
    while (inputBuffer.remaining() > 0) {
      int length = Math.min(random().nextInt(51) + 1, inputBuffer.remaining());
      ByteBuffer inputSlice = inputBuffer.slice();
      inputSlice.limit(inputSlice.position() + length);
      encrypter.process(inputSlice, outputBuffer);
      inputBuffer.position(inputBuffer.position() + length);
    }
    inputBuffer.position(inputInitialPosition);
    outputBuffer.position(0);
    return outputBuffer;
  }

  private static AesCtrEncrypter randomClone(AesCtrEncrypter encrypter) {
    return random().nextBoolean() ? encrypter.clone() : encrypter;
  }
}
