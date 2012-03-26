package org.apache.lucene.analysis.ja.util;

import java.io.IOException;

import org.apache.lucene.analysis.ja.dict.CharacterDefinition;
import org.apache.lucene.analysis.ja.dict.UnknownDictionary;

public class UnknownDictionaryWriter extends BinaryDictionaryWriter {
  private final CharacterDefinitionWriter characterDefinition = new CharacterDefinitionWriter();
  
  public UnknownDictionaryWriter(int size) {
    super(UnknownDictionary.class, size);
  }
  
  @Override
  public int put(String[] entry) {
    // Get wordId of current entry
    int wordId = buffer.position();
    
    // Put entry
    int result = super.put(entry);
    
    // Put entry in targetMap
    int characterId = CharacterDefinition.lookupCharacterClass(entry[0]);
    addMapping(characterId, wordId);
    return result;
  }
  
  /**
   * Put mapping from unicode code point to character class.
   * 
   * @param codePoint code point
   * @param characterClassName character class name
   */
  public void putCharacterCategory(int codePoint, String characterClassName) {
    characterDefinition.putCharacterCategory(codePoint, characterClassName);
  }
  
  public void putInvokeDefinition(String characterClassName, int invoke, int group, int length) {
    characterDefinition.putInvokeDefinition(characterClassName, invoke, group, length);
  }
  
  @Override
  public void write(String baseDir) throws IOException {
    super.write(baseDir);
    characterDefinition.write(baseDir);
  }
}
