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

import static org.apache.lucene.util.crypto.EncryptionUtil.*;

/**
 * {@link AesCtrEncrypter} equivalent to {@link javax.crypto.Cipher} with "AES/CTR/NoPadding" but more efficient.
 * <p>The {@link #LightAesCtrEncrypter(byte[], byte[]) constructor}, the {@link #init(long)} and {@link #clone()}
 * operations are much lighter and fast. But consider {@link CipherAesCtrEncrypter} if you need to load a custom
 * {@link javax.crypto.CipherSpi} implementation.</p>
 *
 * @lucene.experimental
 */
public class LightAesCtrEncrypter implements AesCtrEncrypter {

  /**
   * Factory that creates {@link LightAesCtrEncrypter} instances.
   */
  public static final AesCtrEncrypterFactory FACTORY = new AesCtrEncrypterFactory() {
    @Override
    public AesCtrEncrypter create(byte[] key, byte[] iv) {
      return new LightAesCtrEncrypter(key, iv);
    }
  };

  private final int[] aesKey;
  private final int aesRoundsLimit;
  private final byte[] initialIv;
  private byte[] iv;
  private byte[] ctrEncryptedIv;
  private int ctrNumEncryptedBytes;

  /**
   * @param key The encryption key. It is cloned internally, its content is not modified, and no reference to it is kept.
   * @param iv  The Initialization Vector (IV) for the CTR mode. It MUST be random for the effectiveness of the encryption.
   *            It can be public (for example stored clear at the beginning of the encrypted file). It is cloned internally,
   *            its content is not modified, and no reference to it is kept.
   */
  public LightAesCtrEncrypter(byte[] key, byte[] iv) {
    checkAesKey(key);
    aesKey = buildAesKey(key);
    aesRoundsLimit = getRounds(key.length) * 4;
    this.initialIv = iv.clone();
    this.iv = iv.clone();
    ctrEncryptedIv = new byte[AES_BLOCK_SIZE];
  }

  @Override
  public void init(long counter) {
    checkCtrCounter(counter);
    buildAesCtrIv(initialIv, counter, iv);
    ctrNumEncryptedBytes = AES_BLOCK_SIZE;
  }

  @Override
  public void process(ByteBuffer inBuffer, ByteBuffer outBuffer) {
    assert inBuffer.array() != outBuffer.array() : "Input and output buffers must not be backed by the same array";
    int length = inBuffer.remaining();
    if (length > outBuffer.remaining()) {
      throw new IllegalArgumentException("Output buffer does not have enough remaining space (needs " + length + " B)");
    }
    int outPos = outBuffer.position();
    ctrEncrypt(inBuffer.array(), inBuffer.arrayOffset() + inBuffer.position(), length,
        outBuffer.array(), outBuffer.arrayOffset() + outPos);
    inBuffer.position(inBuffer.limit());
    outBuffer.position(outPos + length);
  }

  @Override
  public LightAesCtrEncrypter clone() {
    try {
      LightAesCtrEncrypter clone = (LightAesCtrEncrypter) super.clone();
      // aesKey and initialIv are the same references.
      clone.iv = initialIv.clone();
      clone.ctrEncryptedIv = new byte[AES_BLOCK_SIZE];
      clone.ctrNumEncryptedBytes = 0;
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new Error("Failed to clone " + LightAesCtrEncrypter.class.getSimpleName() + "; this should not happen");
    }
  }

  //-----------------------------------------------------------------------------
  // Copy of com.sun.crypto.provider.CounterMode
  //-----------------------------------------------------------------------------

  /**
   * Copy of {@code com.sun.crypto.provider.CounterMode#implCrypt}.
   */
  private int ctrEncrypt(byte[] in, int inOff, int len, byte[] out, int outOff) {
    int result = len;
    while (len-- > 0) {
      if (ctrNumEncryptedBytes >= AES_BLOCK_SIZE) {
        aesEncrypt(iv, 0, ctrEncryptedIv, 0);
        increment(iv);
        ctrNumEncryptedBytes = 0;
      }
      out[outOff++] = (byte) (in[inOff++] ^ ctrEncryptedIv[ctrNumEncryptedBytes++]);
    }
    return result;
  }

  /**
   * Copy of {@code com.sun.crypto.provider.CounterMode#increment}.
   */
  private static void increment(byte[] iv) {
    int n = iv.length - 1;
    while ((n >= 0) && (++iv[n] == 0)) {
      n--;
    }
  }

  //-----------------------------------------------------------------------------
  // Copy of com.sun.crypto.provider.AESCrypt
  // Modified to remove the decryption part since we only encrypt for CTR mode.
  //-----------------------------------------------------------------------------

  private static int[]
      alog = new int[256],
      log = new int[256];

  private static final byte[]
      S = new byte[256];

  private static final int[]
      T1 = new int[256],
      T2 = new int[256],
      T3 = new int[256],
      T4 = new int[256];

  private static final byte[] rcon = new byte[30];

  // Static code - to initialise S-boxes and T-boxes
  static {
    int ROOT = 0x11B;
    int i, j;

    //
    // produce log and alog tables, needed for multiplying in the
    // field GF(2^m) (generator = 3)
    //
    alog[0] = 1;
    for (i = 1; i < 256; i++) {
      j = (alog[i - 1] << 1) ^ alog[i - 1];
      if ((j & 0x100) != 0) {
        j ^= ROOT;
      }
      alog[i] = j;
    }
    for (i = 1; i < 255; i++) {
      log[alog[i]] = i;
    }
    byte[][] A = new byte[][]
        {
            {1, 1, 1, 1, 1, 0, 0, 0},
            {0, 1, 1, 1, 1, 1, 0, 0},
            {0, 0, 1, 1, 1, 1, 1, 0},
            {0, 0, 0, 1, 1, 1, 1, 1},
            {1, 0, 0, 0, 1, 1, 1, 1},
            {1, 1, 0, 0, 0, 1, 1, 1},
            {1, 1, 1, 0, 0, 0, 1, 1},
            {1, 1, 1, 1, 0, 0, 0, 1}
        };
    byte[] B = new byte[]{0, 1, 1, 0, 0, 0, 1, 1};

    //
    // substitution box based on F^{-1}(x)
    //
    int t;
    byte[][] box = new byte[256][8];
    box[1][7] = 1;
    for (i = 2; i < 256; i++) {
      j = alog[255 - log[i]];
      for (t = 0; t < 8; t++) {
        box[i][t] = (byte) ((j >>> (7 - t)) & 0x01);
      }
    }
    //
    // affine transform:  box[i] <- B + A*box[i]
    //
    byte[][] cox = new byte[256][8];
    for (i = 0; i < 256; i++) {
      for (t = 0; t < 8; t++) {
        cox[i][t] = B[t];
        for (j = 0; j < 8; j++) {
          cox[i][t] ^= A[t][j] * box[i][j];
        }
      }
    }
    //
    // S-boxes
    //
    for (i = 0; i < 256; i++) {
      S[i] = (byte) (cox[i][0] << 7);
      for (t = 1; t < 8; t++) {
        S[i] ^= cox[i][t] << (7 - t);
      }
    }
    //
    // T-boxes
    //
    byte[][] G = new byte[][]{
        {2, 1, 1, 3},
        {3, 2, 1, 1},
        {1, 3, 2, 1},
        {1, 1, 3, 2}
    };
    byte[][] AA = new byte[4][8];
    for (i = 0; i < 4; i++) {
      for (j = 0; j < 4; j++) AA[i][j] = G[i][j];
      AA[i][i + 4] = 1;
    }
    byte pivot, tmp;
    for (i = 0; i < 4; i++) {
      pivot = AA[i][i];
      if (pivot == 0) {
        t = i + 1;
        while ((AA[t][i] == 0) && (t < 4)) {
          t++;
        }
        if (t == 4) {
          throw new RuntimeException("G matrix is not invertible");
        } else {
          for (j = 0; j < 8; j++) {
            tmp = AA[i][j];
            AA[i][j] = AA[t][j];
            AA[t][j] = tmp;
          }
          pivot = AA[i][i];
        }
      }
      for (j = 0; j < 8; j++) {
        if (AA[i][j] != 0) {
          AA[i][j] = (byte)
              alog[(255 + log[AA[i][j] & 0xFF] - log[pivot & 0xFF])
                  % 255];
        }
      }
      for (t = 0; t < 4; t++) {
        if (i != t) {
          for (j = i + 1; j < 8; j++) {
            AA[t][j] ^= mul(AA[i][j], AA[t][i]);
          }
          AA[t][i] = 0;
        }
      }
    }

    int s;
    for (t = 0; t < 256; t++) {
      s = S[t];
      T1[t] = mul4(s, G[0]);
      T2[t] = mul4(s, G[1]);
      T3[t] = mul4(s, G[2]);
      T4[t] = mul4(s, G[3]);
    }
    //
    // round constants
    //
    rcon[0] = 1;
    int r = 1;
    for (t = 1; t < 30; t++) {
      r = mul(2, r);
      rcon[t] = (byte) r;
    }
    log = null;
    alog = null;
  }

  /**
   * Copy of {@code com.sun.crypto.provider.AESCrypt#mul}.
   */
  private static int mul(int a, int b) {
    return (a != 0 && b != 0) ?
        alog[(log[a & 0xFF] + log[b & 0xFF]) % 255] :
        0;
  }

  /**
   * Copy of {@code com.sun.crypto.provider.AESCrypt#mul4}.
   */
  private static int mul4(int a, byte[] b) {
    if (a == 0) return 0;
    a = log[a & 0xFF];
    int a0 = (b[0] != 0) ? alog[(a + log[b[0] & 0xFF]) % 255] & 0xFF : 0;
    int a1 = (b[1] != 0) ? alog[(a + log[b[1] & 0xFF]) % 255] & 0xFF : 0;
    int a2 = (b[2] != 0) ? alog[(a + log[b[2] & 0xFF]) % 255] & 0xFF : 0;
    int a3 = (b[3] != 0) ? alog[(a + log[b[3] & 0xFF]) % 255] & 0xFF : 0;
    return a0 << 24 | a1 << 16 | a2 << 8 | a3;
  }

  /**
   * Copy of {@code com.sun.crypto.provider.AESCrypt#makeSessionKey}.
   */
  private static int[] buildAesKey(byte[] key) {
    int ROUNDS = getRounds(key.length);
    int ROUND_KEY_COUNT = (ROUNDS + 1) * 4;

    int[][] Ke = new int[ROUNDS + 1][4]; // encryption round keys
    // Only encryption for CTR.

    int KC = key.length / 4; // keylen in 32-bit elements

    int[] tk = new int[KC];
    int i, j;

    // copy user material bytes into temporary ints
    for (i = 0, j = 0; i < KC; i++, j += 4) {
      tk[i] = (key[j]) << 24 |
          (key[j + 1] & 0xFF) << 16 |
          (key[j + 2] & 0xFF) << 8 |
          (key[j + 3] & 0xFF);
    }

    // copy values into round key arrays
    int t = 0;
    for (j = 0; (j < KC) && (t < ROUND_KEY_COUNT); j++, t++) {
      Ke[t / 4][t % 4] = tk[j];
    }
    int tt, rconpointer = 0;
    while (t < ROUND_KEY_COUNT) {
      // extrapolate using phi (the round key evolution function)
      tt = tk[KC - 1];
      tk[0] ^= (S[(tt >>> 16) & 0xFF]) << 24 ^
          (S[(tt >>> 8) & 0xFF] & 0xFF) << 16 ^
          (S[(tt) & 0xFF] & 0xFF) << 8 ^
          (S[(tt >>> 24)] & 0xFF) ^
          (rcon[rconpointer++]) << 24;
      if (KC != 8)
        for (i = 1, j = 0; i < KC; i++, j++) tk[i] ^= tk[j];
      else {
        for (i = 1, j = 0; i < KC / 2; i++, j++) tk[i] ^= tk[j];
        tt = tk[KC / 2 - 1];
        tk[KC / 2] ^= (S[(tt) & 0xFF] & 0xFF) ^
            (S[(tt >>> 8) & 0xFF] & 0xFF) << 8 ^
            (S[(tt >>> 16) & 0xFF] & 0xFF) << 16 ^
            (S[(tt >>> 24)]) << 24;
        for (j = KC / 2, i = j + 1; i < KC; i++, j++) tk[i] ^= tk[j];
      }
      // copy values into round key arrays
      for (j = 0; (j < KC) && (t < ROUND_KEY_COUNT); j++, t++) {
        Ke[t / 4][t % 4] = tk[j];
      }
    }

    // Expand the encryption (Ke) round keys into an array of ints.
    return expandToSubKey(Ke);
  }

  /**
   * Copy of {@code com.sun.crypto.provider.AESCrypt#getRounds}.
   */
  private static int getRounds(int keySize) {
    return (keySize >> 2) + 6;
  }

  /**
   * Copy of {@code com.sun.crypto.provider.AESCrypt#expandToSubKey}.
   */
  private static int[] expandToSubKey(int[][] kr) {
    int total = kr.length;
    int[] expK = new int[total << 2];
    // Only encryption for CTR, straight expansion.
    for (int i = 0; i < total; i++) {
      System.arraycopy(kr[i], 0, expK, i << 2, 4);
    }
    return expK;
  }

  /**
   * Copy of {@code com.sun.crypto.provider.AESCrypt#implEncryptBlock}.
   */
  private void aesEncrypt(byte[] in, int inOffset,
                          byte[] out, int outOffset) {
    int keyOffset = 0;
    int t0 = ((in[inOffset++]) << 24 |
        (in[inOffset++] & 0xFF) << 16 |
        (in[inOffset++] & 0xFF) << 8 |
        (in[inOffset++] & 0xFF)) ^ aesKey[keyOffset++];
    int t1 = ((in[inOffset++]) << 24 |
        (in[inOffset++] & 0xFF) << 16 |
        (in[inOffset++] & 0xFF) << 8 |
        (in[inOffset++] & 0xFF)) ^ aesKey[keyOffset++];
    int t2 = ((in[inOffset++]) << 24 |
        (in[inOffset++] & 0xFF) << 16 |
        (in[inOffset++] & 0xFF) << 8 |
        (in[inOffset++] & 0xFF)) ^ aesKey[keyOffset++];
    int t3 = ((in[inOffset++]) << 24 |
        (in[inOffset++] & 0xFF) << 16 |
        (in[inOffset++] & 0xFF) << 8 |
        (in[inOffset] & 0xFF)) ^ aesKey[keyOffset++];

    // apply round transforms
    while (keyOffset < aesRoundsLimit) {
      int a0, a1, a2;
      a0 = T1[(t0 >>> 24)] ^
          T2[(t1 >>> 16) & 0xFF] ^
          T3[(t2 >>> 8) & 0xFF] ^
          T4[(t3) & 0xFF] ^ aesKey[keyOffset++];
      a1 = T1[(t1 >>> 24)] ^
          T2[(t2 >>> 16) & 0xFF] ^
          T3[(t3 >>> 8) & 0xFF] ^
          T4[(t0) & 0xFF] ^ aesKey[keyOffset++];
      a2 = T1[(t2 >>> 24)] ^
          T2[(t3 >>> 16) & 0xFF] ^
          T3[(t0 >>> 8) & 0xFF] ^
          T4[(t1) & 0xFF] ^ aesKey[keyOffset++];
      t3 = T1[(t3 >>> 24)] ^
          T2[(t0 >>> 16) & 0xFF] ^
          T3[(t1 >>> 8) & 0xFF] ^
          T4[(t2) & 0xFF] ^ aesKey[keyOffset++];
      t0 = a0;
      t1 = a1;
      t2 = a2;
    }

    // last round is special
    int tt = aesKey[keyOffset++];
    out[outOffset++] = (byte) (S[(t0 >>> 24)] ^ (tt >>> 24));
    out[outOffset++] = (byte) (S[(t1 >>> 16) & 0xFF] ^ (tt >>> 16));
    out[outOffset++] = (byte) (S[(t2 >>> 8) & 0xFF] ^ (tt >>> 8));
    out[outOffset++] = (byte) (S[(t3) & 0xFF] ^ (tt));
    tt = aesKey[keyOffset++];
    out[outOffset++] = (byte) (S[(t1 >>> 24)] ^ (tt >>> 24));
    out[outOffset++] = (byte) (S[(t2 >>> 16) & 0xFF] ^ (tt >>> 16));
    out[outOffset++] = (byte) (S[(t3 >>> 8) & 0xFF] ^ (tt >>> 8));
    out[outOffset++] = (byte) (S[(t0) & 0xFF] ^ (tt));
    tt = aesKey[keyOffset++];
    out[outOffset++] = (byte) (S[(t2 >>> 24)] ^ (tt >>> 24));
    out[outOffset++] = (byte) (S[(t3 >>> 16) & 0xFF] ^ (tt >>> 16));
    out[outOffset++] = (byte) (S[(t0 >>> 8) & 0xFF] ^ (tt >>> 8));
    out[outOffset++] = (byte) (S[(t1) & 0xFF] ^ (tt));
    tt = aesKey[keyOffset];
    out[outOffset++] = (byte) (S[(t3 >>> 24)] ^ (tt >>> 24));
    out[outOffset++] = (byte) (S[(t0 >>> 16) & 0xFF] ^ (tt >>> 16));
    out[outOffset++] = (byte) (S[(t1 >>> 8) & 0xFF] ^ (tt >>> 8));
    out[outOffset] = (byte) (S[(t2) & 0xFF] ^ (tt));
  }
}
