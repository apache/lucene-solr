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
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class CipherPool {

  public static final String AES_CTR_TRANSFORMATION = "AES/CTR/NoPadding";

  private final Map<String, SpecificCipherPool> cipherPools = new HashMap<>();

  public CipherPool() {
  }

  public synchronized Cipher get(String transformation) {
    return cipherPools.computeIfAbsent(transformation, SpecificCipherPool::new).get();
  }

  public synchronized void release(Cipher cipher) {
    cipherPools.computeIfAbsent(cipher.getAlgorithm(), (t)->{throw invalidCipherReleased();}).release(cipher);
  }

  private static IllegalArgumentException invalidCipherReleased() {
    return new IllegalArgumentException("Only a Cipher provided by this pool can be released");
  }

  private static class SpecificCipherPool {

    final String transformation;

    /** Available Ciphers in the pool. */
    final Queue<Cipher> availableCiphers = new LinkedList<>();

    /** Security to verify the released Ciphers have been created by this pool. */
    final IdentityHashMap<Cipher,Cipher> providedCiphers = new IdentityHashMap<>();

    SpecificCipherPool(String transformation) {
      this.transformation = transformation;
    }

    Cipher get() {
      if (availableCiphers.isEmpty()) {
        availableCiphers.add(createCipher());
      }
      Cipher cipher = availableCiphers.remove();
      Cipher previous = providedCiphers.put(cipher, cipher);
      assert previous == null;
      if (providedCiphers.size() > 1000) {//TODO nocommit
//        throw new RuntimeException("TOO MANY CIPHERS");
      }
      return cipher;
    }

    void release(Cipher cipher) {
      if (providedCiphers.remove(cipher) == null) {
        throw invalidCipherReleased();
      }
      availableCiphers.add(cipher);
    }

    Cipher createCipher() {
      try {
        return Cipher.getInstance(transformation);
      } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
