package org.apache.solr.util;

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

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**A utility class to verify signatures
 *
 */
public final class CryptoKeys {
  private static final Logger log = LoggerFactory.getLogger(CryptoKeys.class);

  private final Map<String, PublicKey> keys;


  public CryptoKeys(Map<String, byte[]> trustedKeys) throws Exception {
    HashMap<String, PublicKey> m = new HashMap<>();
    for (Map.Entry<String, byte[]> e : trustedKeys.entrySet()) {
      m.put(e.getKey(), getX509PublicKey(e.getValue()));

    }
    this.keys = m;
  }

  /**
   * Try with all signatures and return the name of the signature that matched
   */
  public String verify(String sig, byte[] data) {

    for (Map.Entry<String, PublicKey> entry : keys.entrySet()) {
      boolean verified;
      try {
        verified = CryptoKeys.verify(entry.getValue(), Base64.base64ToByteArray(sig), ByteBuffer.wrap(data));
        log.info("verified {} ", verified);
        if (verified) return entry.getKey();
      } catch (Exception e) {
        log.info("NOT verified  ");
      }

    }

    return null;
  }


  /**
   * Create PublicKey from a .DER file
   */
  public static PublicKey getX509PublicKey(byte[] buf)
      throws Exception {
    X509EncodedKeySpec spec = new X509EncodedKeySpec(buf);
    KeyFactory kf = KeyFactory.getInstance("RSA");
    return kf.generatePublic(spec);
  }

  /**
   * Verify the signature of a file
   *
   * @param publicKey the public key used to sign this
   * @param sig       the signature
   * @param data      The data tha is signed
   */
  public static boolean verify(PublicKey publicKey, byte[] sig, ByteBuffer data) throws InvalidKeyException, SignatureException {
    Signature signature = null;
    try {
      signature = Signature.getInstance("SHA1withRSA");
      signature.initVerify(publicKey);
      signature.update(data);
      boolean verify = signature.verify(sig);
      return verify;

    } catch (NoSuchAlgorithmException e) {
      //will not happen
    }
    return false;
  }


}
