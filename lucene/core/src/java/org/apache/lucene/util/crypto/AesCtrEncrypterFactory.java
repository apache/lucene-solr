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

/**
 * {@link AesCtrEncrypter} factory.
 * <p>Set System property {@link #CRYPTO_CIPHER_PROPERTY} to true for this factory to create {@link javax.crypto.Cipher}-based
 * {@link CipherAesCtrEncrypter} (for example to load custom {@link javax.crypto.CipherSpi} implementation from the classpath).
 * Otherwise by default, it creates light and fast {@link LightAesCtrEncrypter}.</p>
 *
 * @lucene.experimental
 */
public class AesCtrEncrypterFactory {

  private static final AesCtrEncrypterFactory INSTANCE = new AesCtrEncrypterFactory();

  public static final String CRYPTO_CIPHER_PROPERTY = "crypto.cipher";

  private static final boolean CREATE_CRYPTO_CIPHER = Boolean.getBoolean(CRYPTO_CIPHER_PROPERTY);

  public static AesCtrEncrypterFactory getInstance() {
    return INSTANCE;
  }

  protected AesCtrEncrypterFactory() {
  }

  /**
   * Creates a new {@link AesCtrEncrypter} instance.
   * <p>By default it is a {@link LightAesCtrEncrypter}, but this can be modified by setting the System property
   * {@link #CRYPTO_CIPHER_PROPERTY} to true. See {@link AesCtrEncrypterFactory} doc.</p>
   *
   * @param key The encryption key. It is cloned internally, its content is not modified, and no reference to it is kept.
   * @param iv  The Initialization Vector (IV) for the CTR mode. It MUST be random for the effectiveness of the encryption.
   *            It can be public (for example stored clear at the beginning of the encrypted file). It is cloned internally,
   *            its content is not modified, and no reference to it is kept.
   */
  public AesCtrEncrypter create(byte[] key, byte[] iv) {
    return CREATE_CRYPTO_CIPHER ? new CipherAesCtrEncrypter(key, iv) : new LightAesCtrEncrypter(key, iv);
  }
}
