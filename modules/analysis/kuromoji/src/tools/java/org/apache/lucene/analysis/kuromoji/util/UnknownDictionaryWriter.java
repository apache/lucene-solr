package org.apache.lucene.analysis.kuromoji.util;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.kuromoji.dict.CharacterDefinition;
import org.apache.lucene.analysis.kuromoji.dict.UnknownDictionary;

public class UnknownDictionaryWriter extends TokenInfoDictionaryWriter {
  private final CharacterDefinition characterDefinition = new CharacterDefinition();
  
  public UnknownDictionaryWriter(int size) {
    super(size);
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
  
  /**
   * Write dictionary in file
   * Dictionary format is:
   * [Size of dictionary(int)], [entry:{left id(short)}{right id(short)}{word cost(short)}{length of pos info(short)}{pos info(char)}], [entry...], [entry...].....
   * @throws IOException
   */
  public void write(String baseDir) throws IOException {
    writeDictionary(baseDir + File.separator + UnknownDictionary.class.getName().replace('.', File.separatorChar) + UnknownDictionary.DICT_FILENAME_SUFFIX);
    writeTargetMap(baseDir + File.separator + UnknownDictionary.class.getName().replace('.', File.separatorChar) + UnknownDictionary.TARGETMAP_FILENAME_SUFFIX);
    characterDefinition.write(baseDir);
  }
}
