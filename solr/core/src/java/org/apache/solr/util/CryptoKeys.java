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
package org.apache.solr.util;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**A utility class to verify signatures
 *
 */
public final class CryptoKeys {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Map<String, PublicKey> keys;
  private Exception exception;

  public CryptoKeys(Map<String, byte[]> trustedKeys) throws Exception {
    HashMap<String, PublicKey> m = new HashMap<>();
    for (Map.Entry<String, byte[]> e : trustedKeys.entrySet()) {
      m.put(e.getKey(), getX509PublicKey(e.getValue()));

    }
    this.keys = ImmutableMap.copyOf(m);
  }

  /**
   * Try with all signatures and return the name of the signature that matched
   */
  public String verify(String sig, ByteBuffer data) {
    exception = null;
    for (Map.Entry<String, PublicKey> entry : keys.entrySet()) {
      boolean verified;
      try {
        verified = CryptoKeys.verify(entry.getValue(), Base64.base64ToByteArray(sig), data);
        log.debug("verified {} ", verified);
        if (verified) return entry.getKey();
      } catch (Exception e) {
        exception = e;
        log.debug("NOT verified  ");
      }

    }

    return null;
  }

  public String verify(String sig, InputStream is) {
    exception = null;
    for (Map.Entry<String, PublicKey> entry : keys.entrySet()) {
      boolean verified;
      try {
        verified = CryptoKeys.verify(entry.getValue(), Base64.base64ToByteArray(sig), is);
        log.debug("verified {} ", verified);
        if (verified) return entry.getKey();
      } catch (Exception e) {
        exception = e;
        log.debug("NOT verified  ");
      }

    }

    return null;
  }




  /**
   * Create PublicKey from a .DER file
   */
  public static PublicKey getX509PublicKey(byte[] buf)
      throws InvalidKeySpecException {
    X509EncodedKeySpec spec = new X509EncodedKeySpec(buf);
    try {
      KeyFactory kf = KeyFactory.getInstance("RSA");
      return kf.generatePublic(spec);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError("JVM spec is required to support RSA", e);
    }
  }

  /**
   * Verify the signature of a file
   *
   * @param publicKey the public key used to sign this
   * @param sig       the signature
   * @param data      The data tha is signed
   */
  public static boolean verify(PublicKey publicKey, byte[] sig, ByteBuffer data) throws InvalidKeyException, SignatureException {
    data = ByteBuffer.wrap(data.array(), data.arrayOffset(), data.limit());
    try {
      Signature signature = Signature.getInstance("SHA1withRSA");
      signature.initVerify(publicKey);
      signature.update(data);
      return signature.verify(sig);
    } catch (NoSuchAlgorithmException e) {
      //wil not happen
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }

  public static boolean verify(PublicKey publicKey, byte[] sig, InputStream is)
      throws InvalidKeyException, SignatureException, IOException {
    try {
      Signature signature = Signature.getInstance("SHA1withRSA");
      signature.initVerify(publicKey);
      byte[] buf = new byte[1024];
      while (true) {
        int sz = is.read(buf);
        if (sz == -1) break;
        signature.update(buf, 0, sz);
      }
      try {
        return signature.verify(sig);
      } catch (SignatureException e) {
        return false;
      }
    } catch (NoSuchAlgorithmException e) {
      //will not happen
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }


  private static byte[][] evpBytesTokey(int key_len, int iv_len, MessageDigest md,
                                        byte[] salt, byte[] data, int count) {
    byte[][] both = new byte[2][];
    byte[] key = new byte[key_len];
    int key_ix = 0;
    byte[] iv = new byte[iv_len];
    int iv_ix = 0;
    both[0] = key;
    both[1] = iv;
    byte[] md_buf = null;
    int nkey = key_len;
    int niv = iv_len;
    int i = 0;
    if (data == null) {
      return both;
    }
    int addmd = 0;
    for (; ; ) {
      md.reset();
      if (addmd++ > 0) {
        md.update(md_buf);
      }
      md.update(data);
      if (null != salt) {
        md.update(salt, 0, 8);
      }
      md_buf = md.digest();
      for (i = 1; i < count; i++) {
        md.reset();
        md.update(md_buf);
        md_buf = md.digest();
      }
      i = 0;
      if (nkey > 0) {
        for (; ; ) {
          if (nkey == 0)
            break;
          if (i == md_buf.length)
            break;
          key[key_ix++] = md_buf[i];
          nkey--;
          i++;
        }
      }
      if (niv > 0 && i != md_buf.length) {
        for (; ; ) {
          if (niv == 0)
            break;
          if (i == md_buf.length)
            break;
          iv[iv_ix++] = md_buf[i];
          niv--;
          i++;
        }
      }
      if (nkey == 0 && niv == 0) {
        break;
      }
    }
    for (i = 0; i < md_buf.length; i++) {
      md_buf[i] = 0;
    }
    return both;
  }

  public static String decodeAES(String base64CipherTxt, String pwd) {
    int[] strengths = new int[]{256, 192, 128};
    Exception e = null;
    for (int strength : strengths) {
      try {
        return decodeAES(base64CipherTxt, pwd, strength);
      } catch (Exception exp) {
        e = exp;
      }
    }
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error decoding ", e);
  }


  public static String decodeAES(String base64CipherTxt, String pwd, final int keySizeBits) {
    final Charset ASCII = StandardCharsets.US_ASCII;
    final int INDEX_KEY = 0;
    final int INDEX_IV = 1;
    final int ITERATIONS = 1;
    final int SALT_OFFSET = 8;
    final int SALT_SIZE = 8;
    final int CIPHERTEXT_OFFSET = SALT_OFFSET + SALT_SIZE;

    try {
      byte[] headerSaltAndCipherText = Base64.base64ToByteArray(base64CipherTxt);

      // --- extract salt & encrypted ---
      // header is "Salted__", ASCII encoded, if salt is being used (the default)
      byte[] salt = Arrays.copyOfRange(
          headerSaltAndCipherText, SALT_OFFSET, SALT_OFFSET + SALT_SIZE);
      byte[] encrypted = Arrays.copyOfRange(
          headerSaltAndCipherText, CIPHERTEXT_OFFSET, headerSaltAndCipherText.length);

      // --- specify cipher and digest for evpBytesTokey method ---

      Cipher aesCBC = Cipher.getInstance("AES/CBC/PKCS5Padding");
      MessageDigest md5 = MessageDigest.getInstance("MD5");

      // --- create key and IV  ---

      // the IV is useless, OpenSSL might as well have use zero's
      final byte[][] keyAndIV = evpBytesTokey(
          keySizeBits / Byte.SIZE,
          aesCBC.getBlockSize(),
          md5,
          salt,
          pwd.getBytes(ASCII),
          ITERATIONS);

      SecretKeySpec key = new SecretKeySpec(keyAndIV[INDEX_KEY], "AES");
      IvParameterSpec iv = new IvParameterSpec(keyAndIV[INDEX_IV]);

      // --- initialize cipher instance and decrypt ---

      aesCBC.init(Cipher.DECRYPT_MODE, key, iv);
      byte[] decrypted = aesCBC.doFinal(encrypted);
      return new String(decrypted, ASCII);
    } catch (BadPaddingException e) {
      // AKA "something went wrong"
      throw new IllegalStateException(
          "Bad password, algorithm, mode or padding;" +
              " no salt, wrong number of iterations or corrupted ciphertext.", e);
    } catch (IllegalBlockSizeException e) {
      throw new IllegalStateException(
          "Bad algorithm, mode or corrupted (resized) ciphertext.", e);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  public static PublicKey deserializeX509PublicKey(String pubKey) {
    try {
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(Base64.base64ToByteArray(pubKey));
      return keyFactory.generatePublic(publicKeySpec);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,e);
    }
  }

  public static byte[] decryptRSA(byte[] buffer, PublicKey pubKey) throws InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
    Cipher rsaCipher;
    try {
      rsaCipher = Cipher.getInstance("RSA/ECB/nopadding");
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,e);
    }
    rsaCipher.init(Cipher.DECRYPT_MODE, pubKey);
    return rsaCipher.doFinal(buffer, 0, buffer.length);
  }

  public static class RSAKeyPair {
    private final String pubKeyStr;
    private final PublicKey publicKey;
    private final PrivateKey privateKey;

    // If this ever comes back to haunt us see the discussion at
    // SOLR-9609 for background and code allowing this to go
    // into security.json. Also see SOLR-12103.
    private static final int DEFAULT_KEYPAIR_LENGTH = 2048;

    /**
     * Create an RSA key pair with newly generated keys.
     */
    public RSAKeyPair() {
      KeyPairGenerator keyGen;
      try {
        keyGen = KeyPairGenerator.getInstance("RSA");
      } catch (NoSuchAlgorithmException e) {
        throw new AssertionError("JVM spec is required to support RSA", e);
      }
      keyGen.initialize(DEFAULT_KEYPAIR_LENGTH);
      java.security.KeyPair keyPair = keyGen.genKeyPair();
      privateKey = keyPair.getPrivate();
      publicKey = keyPair.getPublic();
      pubKeyStr = Base64.byteArrayToBase64(publicKey.getEncoded());
    }

    /**
     * Initialize an RSA key pair from previously saved keys. The formats listed below have been tested, other formats may
     * also be acceptable but are not guaranteed to work.
     * @param privateKeyResourceName path to private key file, encoded as a PKCS#8 in a PEM file
     * @param publicKeyResourceName path to public key file, encoded as X509 in a DER file
     * @throws IOException if an I/O error occurs reading either key file
     * @throws InvalidKeySpecException if either key file is inappropriate for an RSA key
     */
    public RSAKeyPair(URL privateKeyResourceName, URL publicKeyResourceName) throws IOException, InvalidKeySpecException {
      try (InputStream inPrivate = privateKeyResourceName.openStream()) {
        String privateString = new String(inPrivate.readAllBytes(), StandardCharsets.UTF_8)
            .replaceAll("-----(BEGIN|END) PRIVATE KEY-----", "");

        PKCS8EncodedKeySpec privateSpec = new PKCS8EncodedKeySpec(java.util.Base64.getMimeDecoder().decode(privateString));
        KeyFactory rsaFactory = KeyFactory.getInstance("RSA");
        privateKey = rsaFactory.generatePrivate(privateSpec);
      } catch (NoSuchAlgorithmException e) {
        throw new AssertionError("JVM spec is required to support RSA", e);
      }

      try (InputStream inPublic = publicKeyResourceName.openStream()) {
        publicKey = getX509PublicKey(inPublic.readAllBytes());
        pubKeyStr = Base64.byteArrayToBase64(publicKey.getEncoded());
      }
    }

    public String getPublicKeyStr() {
      return pubKeyStr;
    }

    public PublicKey getPublicKey() {
      return publicKey;
    }

    public byte[] encrypt(ByteBuffer buffer) {
      try {
        // This is better than nothing, but still not very secure
        // See: https://crypto.stackexchange.com/questions/20085/which-attacks-are-possible-against-raw-textbook-rsa
        Cipher rsaCipher = Cipher.getInstance("RSA/ECB/nopadding");
        rsaCipher.init(Cipher.ENCRYPT_MODE, privateKey);
        return rsaCipher.doFinal(buffer.array(),buffer.position(), buffer.limit());
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,e);
      }
    }
    public byte[] signSha256(byte[] bytes) throws InvalidKeyException, SignatureException {
      Signature dsa = null;
      try {
        dsa = Signature.getInstance("SHA256withRSA");
      } catch (NoSuchAlgorithmException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
      dsa.initSign(privateKey);
      dsa.update(bytes,0,bytes.length);
      return dsa.sign();

    }

  }
}
