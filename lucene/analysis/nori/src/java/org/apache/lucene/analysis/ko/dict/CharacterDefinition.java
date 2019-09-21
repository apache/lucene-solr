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
package org.apache.lucene.analysis.ko.dict;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOUtils;

/**
 * Character category data.
 */
public final class CharacterDefinition {

  public static final String FILENAME_SUFFIX = ".dat";
  public static final String HEADER = "ko_cd";
  public static final int VERSION = 1;

  public static final int CLASS_COUNT = CharacterClass.values().length;

  // only used internally for lookup:
  enum CharacterClass {
    NGRAM, DEFAULT, SPACE, SYMBOL, NUMERIC, ALPHA, CYRILLIC, GREEK, HIRAGANA, KATAKANA, KANJI, HANGUL, HANJA, HANJANUMERIC
  }

  private final byte[] characterCategoryMap = new byte[0x10000];

  private final boolean[] invokeMap = new boolean[CLASS_COUNT];
  private final boolean[] groupMap = new boolean[CLASS_COUNT];

  // the classes:
  public static final byte NGRAM = (byte) CharacterClass.NGRAM.ordinal();
  public static final byte DEFAULT = (byte) CharacterClass.DEFAULT.ordinal();
  public static final byte SPACE = (byte) CharacterClass.SPACE.ordinal();
  public static final byte SYMBOL = (byte) CharacterClass.SYMBOL.ordinal();
  public static final byte NUMERIC = (byte) CharacterClass.NUMERIC.ordinal();
  public static final byte ALPHA = (byte) CharacterClass.ALPHA.ordinal();
  public static final byte CYRILLIC = (byte) CharacterClass.CYRILLIC.ordinal();
  public static final byte GREEK = (byte) CharacterClass.GREEK.ordinal();
  public static final byte HIRAGANA = (byte) CharacterClass.HIRAGANA.ordinal();
  public static final byte KATAKANA = (byte) CharacterClass.KATAKANA.ordinal();
  public static final byte KANJI = (byte) CharacterClass.KANJI.ordinal();
  public static final byte HANGUL = (byte) CharacterClass.HANGUL.ordinal();
  public static final byte HANJA = (byte) CharacterClass.HANJA.ordinal();
  public static final byte HANJANUMERIC = (byte) CharacterClass.HANJANUMERIC.ordinal();
  
  private CharacterDefinition() throws IOException {
    InputStream is = null;
    boolean success = false;
    try {
      is = BinaryDictionary.getClassResource(getClass(), FILENAME_SUFFIX);
      is = new BufferedInputStream(is);
      final DataInput in = new InputStreamDataInput(is);
      CodecUtil.checkHeader(in, HEADER, VERSION, VERSION);
      in.readBytes(characterCategoryMap, 0, characterCategoryMap.length);
      for (int i = 0; i < CLASS_COUNT; i++) {
        final byte b = in.readByte();
        invokeMap[i] = (b & 0x01) != 0;
        groupMap[i] = (b & 0x02) != 0;
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(is);
      } else {
        IOUtils.closeWhileHandlingException(is);
      }
    }
  }
  
  public byte getCharacterClass(char c) {
    return characterCategoryMap[c];
  }
  
  public boolean isInvoke(char c) {
    return invokeMap[characterCategoryMap[c]];
  }
  
  public boolean isGroup(char c) {
    return groupMap[characterCategoryMap[c]];
  }

  public boolean isHanja(char c) {
    final byte characterClass = getCharacterClass(c);
    return characterClass == HANJA || characterClass == HANJANUMERIC;
  }

  public boolean isHangul(char c) {
    return getCharacterClass(c) == HANGUL;
  }

  public boolean hasCoda(char ch){
    return ((ch - 0xAC00) % 0x001C) != 0;
  }

  public static byte lookupCharacterClass(String characterClassName) {
    return (byte) CharacterClass.valueOf(characterClassName).ordinal();
  }

  public static CharacterDefinition getInstance() {
    return SingletonHolder.INSTANCE;
  }
  
  private static class SingletonHolder {
    static final CharacterDefinition INSTANCE;
    static {
      try {
        INSTANCE = new CharacterDefinition();
      } catch (IOException ioe) {
        throw new RuntimeException("Cannot load CharacterDefinition.", ioe);
      }
    }
   }
}
