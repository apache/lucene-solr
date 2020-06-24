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

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class EncryptingUtil {

  /**
   * AES block has a fixed length of 16 bytes (128 bits).
   */
  public static final int AES_BLOCK_SIZE = 16;

  /**
   * AES/CTR IV length. It is equal to {@link #AES_BLOCK_SIZE}. It is defined separately mainly for code clarity.
   */
  public static final int IV_LENGTH = AES_BLOCK_SIZE;

  public static final String AES_CTR_TRANSFORMATION = "AES/CTR/NoPadding";

  /**
   * Creates a secret key for AES using the bytes provided.
   * @param key The key bytes (cloned in the key). {@code key.length} must be either 16, 24 or 32.
   */
  public static SecretKeySpec createAesKey(byte[] key) throws IOException {
    if (key.length != 16 && key.length != 24 && key.length != 32) {
      // AES requires either 128, 192 or 256 bits keys.
      throw new IOException("Invalid AES secret key length; it must be either 128, 192 or 256 bits long");
    }
    return new SecretKeySpec(key, "AES");
  }

  /**
   * Generates a random IV for AES/CTR of length {@link #IV_LENGTH}.
   */
  public static byte[] generateRandomAesCtrIv() {
    // IV length must be the AES block size.
    // IV must be random for the CTR mode. It starts with counter 0, so it's simply IV.
    // The CTR counter will be added during calls to Cipher.update(). See com.sun.crypto.provider.CounterMode.
    byte[] iv = new byte[IV_LENGTH];
    getSecureRandom().nextBytes(iv);

    // Ensure that we have at least a couple bits left to guarantee that the 8 bytes counter can add with the carry.
    boolean bitLeft = false;
    for (int i = 0; i < 8; i++) {
      if (iv[i] != -1) {
        bitLeft = true;
        break;
      }
    }
    if (!bitLeft) {
      iv[7] = -4;
    }
    return iv;
  }

  /**
   * Builds an AES/CTR IV based on the provided counter and an initial IV.
   * The built IV is compatible with com.sun.crypto.provider.CounterMode.increment().
   */
  public static void buildAesCtrIv(byte[] initialIv, long counter, byte[] iv) {
    assert initialIv.length == IV_LENGTH && iv.length == IV_LENGTH;
    int ivIndex = iv.length;
    int counterIndex = 0;
    int sum = 0;
    while (ivIndex-- > 0) {
      // (sum >>> Byte.SIZE) is the carry for counter addition.
      sum = (initialIv[ivIndex] & 0xff) + (sum >>> Byte.SIZE);
      // Add long counter.
      if (counterIndex++ < 8) {
        sum += (byte) counter & 0xff;
        counter >>>= 8;
      }
      iv[ivIndex] = (byte) sum;
    }
  }

  public static Cipher createAesCtrCipher() {
    try {
      return Cipher.getInstance(AES_CTR_TRANSFORMATION);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new RuntimeException(e);
    }
  }

  private static SecureRandom getSecureRandom() {
    return SecureRandomHolder.SECURE_RANDOM;
  }

  private static class SecureRandomHolder {
    static final SecureRandom SECURE_RANDOM = new SecureRandom();
  }
}
