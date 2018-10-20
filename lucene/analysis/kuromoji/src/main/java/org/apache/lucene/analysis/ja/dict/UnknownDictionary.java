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
package org.apache.lucene.analysis.ja.dict;


import java.io.IOException;

/**
 * Dictionary for unknown-word handling.
 */
public final class UnknownDictionary extends BinaryDictionary {

  private final CharacterDefinition characterDefinition = CharacterDefinition.getInstance();
  
  private UnknownDictionary() throws IOException {
    super();
  }
  
  public int lookup(char[] text, int offset, int len) {
    if(!characterDefinition.isGroup(text[offset])) {
      return 1;
    }
    
    // Extract unknown word. Characters with the same character class are considered to be part of unknown word
    byte characterIdOfFirstCharacter = characterDefinition.getCharacterClass(text[offset]);
    int length = 1;
    for (int i = 1; i < len; i++) {
      if (characterIdOfFirstCharacter == characterDefinition.getCharacterClass(text[offset+i])){
        length++;
      } else {
        break;
      }
    }
    
    return length;
  }
  
  public CharacterDefinition getCharacterDefinition() {
    return characterDefinition;
  }
  
  @Override
  public String getReading(int wordId, char surface[], int off, int len) {
    return null;
  }

  @Override
  public String getInflectionType(int wordId) {
    return null;
  }

  @Override
  public String getInflectionForm(int wordId) {
    return null;
  }

  public static UnknownDictionary getInstance() {
    return SingletonHolder.INSTANCE;
  }
  
  private static class SingletonHolder {
    static final UnknownDictionary INSTANCE;
    static {
      try {
        INSTANCE = new UnknownDictionary();
      } catch (IOException ioe) {
        throw new RuntimeException("Cannot load UnknownDictionary.", ioe);
      }
    }
   }
  
}
