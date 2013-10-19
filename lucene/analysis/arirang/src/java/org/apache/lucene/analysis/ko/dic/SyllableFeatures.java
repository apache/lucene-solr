package org.apache.lucene.analysis.ko.dic;

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

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

public class SyllableFeatures {
  private SyllableFeatures() {}

  /** 조사의 첫음절로 사용되는 음절 49개 */
  public static int JOSA1 = 0;
  /** 조사의 두 번째 이상의 음절로 사용되는 음절 58개 */
  public static int JOSA2 = 1;
  /** 어미의 두 번째 이상의 음절로 사용되는 음절 105개 */
  public static int EOMI2 = 2;
  /** (용언+'-ㄴ')에 의하여 생성되는 음절 129개 */
  public static int YNPNA = 3;
  /** (용언+'-ㄹ')에 의해 생성되는 음절 129개 */
  public static int YNPLA = 4;
  /** (용언+'-ㅁ')에 의해 생성되는 음절 129개 */
  public static int YNPMA = 5;
  /** (용언+'-ㅂ')에 의해 생성되는 음절 129개 */
  public static int YNPBA = 6;
  /** 모음으로 끝나는 음절 129개중 'ㅏ/ㅓ/ㅐ/ㅔ/ㅕ'로 끝나는 것이 선어말 어미 '-었-'과 결합할 때 생성되는 음절 */
  public static int YNPAH = 7;
  /** 받침 'ㄹ'로 끝나는 용언이 어미 '-ㄴ'과 결합할 때 생성되는 음절 */
  public static int YNPLN = 8;
  /** 용언의 표층 형태로만 사용되는 음절 */
  public static int WDSURF = 9;
  /** 어미 또는 어미의 변형으로 존재할 수 있는 음 (즉 IDX_EOMI 이거나 IDX_YNPNA 이후에 1이 있는 음절) */
  public static int EOGAN = 10;
  
  private static final int NUM_FEATURES = 11;
  private static final int HANGUL_START = 0xAC00;
  private static final int HANGUL_END = 0xD7AF;
  
  private static final FixedBitSet features;
  static {
    InputStream stream = null;
    try {
      stream = DictionaryResources.class.getResourceAsStream(DictionaryResources.FILE_SYLLABLE_DAT);
      if (stream == null)
        throw new FileNotFoundException(DictionaryResources.FILE_SYLLABLE_DAT);
      DataInput dat = new InputStreamDataInput(new BufferedInputStream(stream));
      CodecUtil.checkHeader(dat, DictionaryResources.FILE_SYLLABLE_DAT, DictionaryResources.DATA_VERSION, DictionaryResources.DATA_VERSION);
      long bits[] = new long[dat.readVInt()];
      for (int i = 0; i < bits.length; i++) {
        bits[i] = dat.readLong();
      }
      features = new FixedBitSet(bits, (1 + HANGUL_END - HANGUL_START) * NUM_FEATURES);
    } catch (IOException ioe) {
      throw new Error("Cannot load ressource", ioe);
    } finally {
      IOUtils.closeWhileHandlingException(stream);
    }
  }
  
  /** Returns true if the syllable has the specified feature */
  public static boolean hasFeature(char syl, int feature) {
    if (syl < HANGUL_START || syl > HANGUL_END) {
      return false; // outside of hangul syllable range
    } else {
      return features.get((syl - HANGUL_START) * NUM_FEATURES + feature);
    }
  }
}
