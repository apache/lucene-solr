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

import java.io.IOException;

public class UnknownDictionary extends BinaryDictionary {

  private final CharacterDefinition characterDefinition = CharacterDefinition.getInstance();
  
  private UnknownDictionary() throws IOException {
    super();
  }
  
  public int lookup(String text) {
    if(!characterDefinition.isGroup(text.charAt(0))) {
      return 1;
    }
    
    // Extract unknown word. Characters with the same character class are considered to be part of unknown word
    byte characterIdOfFirstCharacter = characterDefinition.getCharacterClass(text.charAt(0));
    int length = 1;
    for (int i = 1; i < text.length(); i++) {
      if (characterIdOfFirstCharacter == characterDefinition.getCharacterClass(text.charAt(i))){
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
  public String getReading(int wordId) {
    return null;
  }

  public synchronized static UnknownDictionary getInstance() {
    if (singleton == null) try {
      singleton = new UnknownDictionary();
    } catch (IOException ioe) {
      throw new RuntimeException("Cannot load UnknownDictionary.", ioe);
    }
    return singleton;
  }
  
  private static UnknownDictionary singleton;
  
}
