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

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import static org.apache.lucene.util.crypto.EncryptionUtil.*;

/**
 * Stateful {@link AesCtrEncrypter} backed by a {@link javax.crypto.Cipher} with the "AES/CTR/NoPadding" transformation.
 * <p>This encrypter loads the internal {@link javax.crypto.CipherSpi} implementation from the classpath, see
 * {@link Cipher#getInstance(String)}. It is heavy to create, to initialize and to clone, but the
 * {@link #process(ByteBuffer, ByteBuffer) encryption} is extremely fast thanks to {@code HotSpotIntrinsicCandidate}
 * annotation in com.sun.crypto.provider.CounterMode.</p>
 *
 * @lucene.experimental
 */
public class CipherAesCtrEncrypter implements AesCtrEncrypter {

  /**
   * Factory that creates {@link CipherAesCtrEncrypter} instances.
   */
  public static final AesCtrEncrypterFactory FACTORY = CipherAesCtrEncrypter::new;

  // Most fields are not final for the clone() method.
  private final Key key;
  private final byte[] initialIv;
  private byte[] iv;
  private ReusableIvParameterSpec ivParameterSpec;
  private Cipher cipher;
  private long counter;

  /**
   * @param key The encryption key. It is cloned internally, its content is not modified, and no reference to it is kept.
   * @param iv  The Initialization Vector (IV) for the CTR mode. It MUST be random for the effectiveness of the encryption.
   *            It can be public (for example stored clear at the beginning of the encrypted file). It is cloned internally,
   *            its content is not modified, and no reference to it is kept.
   */
  public CipherAesCtrEncrypter(byte[] key, byte[] iv) {
    checkAesKey(key);
    this.key = new SecretKeySpec(key, "AES");
    this.initialIv = iv.clone();
    this.iv = iv.clone();
    ivParameterSpec = new ReusableIvParameterSpec(this.iv);
    cipher = createAesCtrCipher();
  }

  @Override
  public void init(long counter) {
    checkCtrCounter(counter);
    if (counter != this.counter) {
      this.counter = counter;
      buildAesCtrIv(initialIv, counter, iv);
    }
    try {
      cipher.init(Cipher.ENCRYPT_MODE, key, ivParameterSpec, EncryptionUtil.getSecureRandom());
    } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(ByteBuffer inBuffer, ByteBuffer outBuffer) {
    try {
      int inputSize = inBuffer.remaining();
      int numEncryptedBytes = cipher.update(inBuffer, outBuffer);
      if (numEncryptedBytes < inputSize) {
        throw new UnsupportedOperationException("The " + Cipher.class.getSimpleName() + " implementation does not maintain an encryption context; this is not supported");
      }
    } catch (ShortBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CipherAesCtrEncrypter clone() {
    CipherAesCtrEncrypter clone;
    try {
      clone = (CipherAesCtrEncrypter) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new Error("This cannot happen: Failing to clone " + CipherAesCtrEncrypter.class.getSimpleName());
    }
    // key and initialIv are the same references.
    clone.iv = initialIv.clone();
    clone.ivParameterSpec = new ReusableIvParameterSpec(clone.iv);
    clone.cipher = createAesCtrCipher();
    clone.counter = 0;
    return clone;
  }

  private static Cipher createAesCtrCipher() {
    try {
      Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
      assert cipher.getBlockSize() == AES_BLOCK_SIZE : "Invalid AES block size: " + cipher.getBlockSize();
      return cipher;
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Avoids cloning the IV in the constructor each time we need an {@link IvParameterSpec}.
   */
  private static class ReusableIvParameterSpec extends IvParameterSpec {

    static final byte[] EMPTY_BYTES = new byte[0];

    final byte[] iv;

    ReusableIvParameterSpec(byte[] iv) {
      super(EMPTY_BYTES);
      this.iv = iv;
    }

    public byte[] getIV() {
      return iv.clone();
    }
  }
}
