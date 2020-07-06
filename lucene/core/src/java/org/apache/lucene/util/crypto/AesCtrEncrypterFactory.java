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
 *
 * @lucene.experimental
 */
public interface AesCtrEncrypterFactory {

  /**
   * Creates a new {@link AesCtrEncrypter} instance.
   *
   * @param key The encryption key. It is cloned internally, its content is not modified, and no reference to it is kept.
   * @param iv  The Initialization Vector (IV) for the CTR mode. It MUST be random for the effectiveness of the encryption.
   *            It can be public (for example stored clear at the beginning of the encrypted file). It is cloned internally,
   *            its content is not modified, and no reference to it is kept.
   */
  AesCtrEncrypter create(byte[] key, byte[] iv);
}
