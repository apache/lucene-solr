package org.apache.lucene.analysis.kuromoji.dict;

/**
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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.CodecUtil;

public final class CharacterDefinition {
  public static final String FILENAME = "cd.dat";
  public static final String HEADER = "kuromoji_cd";
  public static final int VERSION = 1;

  private static final int CLASS_COUNT = CharacterClass.values().length;
  
  // only used internally for lookup:
  private static enum CharacterClass {
    NGRAM, DEFAULT, SPACE, SYMBOL, NUMERIC, ALPHA, CYRILLIC, GREEK, HIRAGANA, KATAKANA, KANJI, KANJINUMERIC;
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
  public static final byte KANJINUMERIC = (byte) CharacterClass.KANJINUMERIC.ordinal();
  
  /**
   * Constructor
   */
  public CharacterDefinition() {
    Arrays.fill(characterCategoryMap, DEFAULT);
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
  
  public boolean isKanji(char c) {
    final byte characterClass = characterCategoryMap[c];
    return characterClass == KANJI || characterClass == KANJINUMERIC;
  }
  
  /**
   * Put mapping from unicode code point to character class.
   * 
   * @param codePoint
   *            code point
   * @param characterClassName character class name
   */
  public void putCharacterCategory(int codePoint, String characterClassName) {
    characterClassName = characterClassName.split(" ")[0]; // use first
    // category
    // class
    
    // Override Nakaguro
    if (codePoint == 0x30FB) {
      characterClassName = "SYMBOL";
    }
    characterCategoryMap[codePoint] = lookupCharacterClass(characterClassName);
  }
  
  public void putInvokeDefinition(String characterClassName, int invoke, int group, int length) {
    final byte characterClass = lookupCharacterClass(characterClassName);
    invokeMap[characterClass] = invoke == 1;
    groupMap[characterClass] = group == 1;
    // TODO: length def ignored
  }
  
  public static byte lookupCharacterClass(String characterClassName) {
    return (byte) CharacterClass.valueOf(characterClassName).ordinal();
  }

  public void write(String directoryname) throws IOException {
    String filename = directoryname + File.separator + FILENAME;
    OutputStream os = new FileOutputStream(filename);
    try {
      os = new BufferedOutputStream(os);
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, HEADER, VERSION);
      out.writeBytes(characterCategoryMap, 0, characterCategoryMap.length);
      for (int i = 0; i < CLASS_COUNT; i++) {
        final byte b = (byte) (
          (invokeMap[i] ? 0x01 : 0x00) | 
          (groupMap[i] ? 0x02 : 0x00)
        );
        out.writeByte(b);
      }
    } finally {
      os.close();
    }
  }
  
  public static CharacterDefinition getInstance() throws IOException, ClassNotFoundException {
    InputStream is = CharacterDefinition.class.getResourceAsStream(FILENAME);
    return read(is);
  }
  
  public static CharacterDefinition read(InputStream is) throws IOException, ClassNotFoundException {
    is = new BufferedInputStream(is);
    try {
      final DataInput in = new InputStreamDataInput(is);
      CodecUtil.checkHeader(in, HEADER, VERSION, VERSION);
      CharacterDefinition cd = new CharacterDefinition();
      in.readBytes(cd.characterCategoryMap, 0, cd.characterCategoryMap.length);
      for (int i = 0; i < CLASS_COUNT; i++) {
        final byte b = in.readByte();
        cd.invokeMap[i] = (b & 0x01) != 0;
        cd.groupMap[i] = (b & 0x02) != 0;
      }
      return cd;
    } finally {
      is.close();
    }
  }

}
