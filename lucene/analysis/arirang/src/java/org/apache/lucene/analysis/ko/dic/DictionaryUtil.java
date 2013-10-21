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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.ko.utils.Trie;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.fst.FST;

public class DictionaryUtil {
  private DictionaryUtil() {}
  
  private static final Trie<String,WordEntry> dictionary = new Trie<String, WordEntry>(false);
  
  private static final HangulDictionary newDictionary;
  
  private static final Set<String> josas = new HashSet<String>();
  
  private static final Set<String> eomis = new HashSet<String>();;
  
  private static final Set<String> prefixs = new HashSet<String>();;
  
  private static final Set<String> suffixs = new HashSet<String>();;
  
  private static final Set<String> uncompounds = new HashSet<String>();
  
  static {  
    try {
      final LineProcessor proc = new LineProcessor() {
        @Override
        public void processLine(String line) throws IOException {
          String[] infos = line.split("[,]+");
          if (infos.length != 2) {
            throw new IOException("Invalid file format: " + line);
          }
          if (infos[1].length() != 10) {
            throw new IOException("Invalid file format: " + line);
          }
          
          WordEntry entry = new WordEntry(infos[0].trim(), parseFlags(infos[1]), null);
          dictionary.add(entry.getWord(), entry);          
        }
      };
      DictionaryResources.readLines(DictionaryResources.FILE_DICTIONARY, proc);
      DictionaryResources.readLines(DictionaryResources.FILE_EXTENSION, proc);
      
      DictionaryResources.readLines(DictionaryResources.FILE_COMPOUNDS, new LineProcessor() {
        @Override
        public void processLine(String compound) throws IOException {
          String[] infos = compound.split("[:]+");
          if (infos.length != 3) {
            throw new IOException("Invalid file format: " + compound);
          }
          if (infos[2].length() != 4) {
            throw new IOException("Illegal file format: " + compound);
          }
          
          final List<CompoundEntry> c = compoundArrayToList(infos[1], infos[1].split("[,]+"));
          final WordEntry entry = new WordEntry(infos[0].trim(), parseFlags("200"+infos[2]+"00X"), c);
          dictionary.add(entry.getWord(), entry);          
        }       
      }); 
      
      DictionaryResources.readLines(DictionaryResources.FILE_UNCOMPOUNDS, new LineProcessor() {
        @Override
        public void processLine(String compound) throws IOException {
          String[] infos = compound.split("[:]+");
          if(infos.length!=2) {
            throw new IOException("Invalid file format: "+compound);
          }
          uncompounds.add(infos[1]);
        }
      });

      readFileToSet(josas,DictionaryResources.FILE_JOSA);
      
      readFileToSet(eomis,DictionaryResources.FILE_EOMI);
      
      readFileToSet(prefixs,DictionaryResources.FILE_PREFIX);
  
      readFileToSet(suffixs,DictionaryResources.FILE_SUFFIX);
      
      InputStream stream = DictionaryResources.class.getResourceAsStream(DictionaryResources.FILE_WORDS_DAT);
      DataInput dat = new InputStreamDataInput(new BufferedInputStream(stream));
      CodecUtil.checkHeader(dat, DictionaryResources.FILE_WORDS_DAT, DictionaryResources.DATA_VERSION, DictionaryResources.DATA_VERSION);
      byte metadata[] = new byte[dat.readByte() * HangulDictionary.RECORD_SIZE];
      dat.readBytes(metadata, 0, metadata.length);
      ByteOutputs outputs = ByteOutputs.getSingleton();
      FST<Byte> fst = new FST<Byte>(dat, outputs);
      newDictionary = new HangulDictionary(fst, metadata);
      stream.close();
    } catch (IOException e) {
      throw new Error("Cannot load resource",e);
    }
  }

  @SuppressWarnings({"rawtypes","unchecked"})
  public static Iterator<String[]> findWithPrefix(String prefix) {
    return dictionary.getPrefixedBy(prefix);
  }

  /** only use this if you surely need the whole entry */
  public static WordEntry getWord(String key) {    
    Byte b = newDictionary.lookup(key);
    if (b == null) {
      return null;
    } else {
      return newDictionary.decodeEntry(key, b);
    }
  }
  
  public static WordEntry getWordExceptVerb(String key) {
    Byte b = newDictionary.lookup(key);
    if (b == null) {
      return null;
    }
    char flags = newDictionary.getFlags(b);
    if ((flags & (WordEntry.NOUN | WordEntry.BUSA)) != 0) {
      return newDictionary.decodeEntry(key, b, flags);
    } else {
      return null;
    }
  }
  
  public static WordEntry getNoun(String key) {
    Byte b = newDictionary.lookup(key);
    if (b == null) {
      return null;
    }
    char flags = newDictionary.getFlags(b);
    if ((flags & WordEntry.NOUN) != 0 && (flags & WordEntry.COMPOUND) == 0) {
      return newDictionary.decodeEntry(key, b, flags);
    } else {
      return null;
    }
  }
  
  /**
   * 
   * return all noun including compound noun
   * @param key the lookup key text
   * @return  WordEntry
   */
  public static WordEntry getAllNoun(String key) {  
    Byte b = newDictionary.lookup(key);
    if (b == null) {
      return null;
    }
    char flags = newDictionary.getFlags(b);
    if ((flags & WordEntry.NOUN) != 0) {
      return newDictionary.decodeEntry(key, b, flags);
    } else {
      return null;
    }
  }
  
  public static WordEntry getVerb(String key) {
    Byte b = newDictionary.lookup(key);
    if (b == null) {
      return null;
    }
    char flags = newDictionary.getFlags(b);
    if ((flags & WordEntry.VERB) != 0) {
      return newDictionary.decodeEntry(key, b, flags);
    } else {
      return null;
    }
  }
  
  public static WordEntry getBusa(String key) {
    Byte b = newDictionary.lookup(key);
    if (b == null) {
      return null;
    }
    char flags = newDictionary.getFlags(b);
    if ((flags & WordEntry.BUSA) != 0 && (flags & WordEntry.NOUN) == 0) {
      return newDictionary.decodeEntry(key, b, flags);
    } else {
      return null;
    }
  }
  
  // TODO: make this more efficient later
  public static boolean isUncompound(String before, String after) {
    return uncompounds.contains(before + "," + after);
  }
  
  public static boolean existJosa(String str) {
    return josas.contains(str);
  }
  
  public static boolean existEomi(String str) {
    return eomis.contains(str);
  }
  
  public static boolean existPrefix(String str) {
    return prefixs.contains(str);
  }
  
  public static boolean existSuffix(String str) {
    return suffixs.contains(str);
  }
  
  /**
   * ㄴ,ㄹ,ㅁ,ㅂ과 eomi 가 결합하여 어미가 될 수 있는지 점검한다.
   */
  public static String combineAndEomiCheck(char s, String eomi) {
  
    if(eomi==null) eomi="";

    if(s=='ㄴ') eomi = "은"+eomi;
    else if(s=='ㄹ') eomi = "을"+eomi;
    else if(s=='ㅁ') eomi = "음"+eomi;
    else if(s=='ㅂ') eomi = "습"+eomi;
    else eomi = s+eomi;

    if(existEomi(eomi)) return eomi;    

    return null;
    
  }
  
  private static void readFileToSet(final Set<String> set, String dic) throws IOException {    
    DictionaryResources.readLines(dic, new LineProcessor() {
      @Override
      public void processLine(String line) {
        set.add(line.trim());
      }
    });
  }
  
  private static List<CompoundEntry> compoundArrayToList(String source, String[] arr) {
    List<CompoundEntry> list = new ArrayList<CompoundEntry>();
    for(String str: arr) {
      list.add(new CompoundEntry(str, true));
    }
    return list;
  }
  
  // TODO: move all this to build time
  private static int parseFlags(String buffer) {
    if (buffer.length() != 10) {
      throw new IllegalArgumentException("Invalid flags: " + buffer);
    }
    int flags = 0;
    // IDX_NOUN: 1 if noun, 2 if compound
    if (buffer.charAt(0) == '2') {
      flags |= WordEntry.COMPOUND | WordEntry.NOUN;
    } else if (buffer.charAt(0) == '1') {
      flags |= WordEntry.NOUN;
    } else if (buffer.charAt(0) != '0') {
      throw new IllegalArgumentException("Invalid flags: " + buffer);
    }
    // IDX_VERB
    if (parseBoolean(buffer, 1)) {
      flags |= WordEntry.VERB;
    }
    // IDX_BUSA
    if (parseBoolean(buffer, 2)) {
      flags |= WordEntry.BUSA;
    }
    // IDX_DOV
    if (parseBoolean(buffer, 3)) {
      flags |= WordEntry.DOV;
    }
    // IDX_BEV
    if (parseBoolean(buffer, 4)) {
      flags |= WordEntry.BEV;
    }
    // IDX_NE
    if (parseBoolean(buffer, 5)) {
      flags |= WordEntry.NE;
    }
    // IDX_REGURA
    switch(buffer.charAt(9)) {
      case 'B': return flags | WordEntry.VERB_TYPE_BIUP;
      case 'H': return flags | WordEntry.VERB_TYPE_HIOOT;
      case 'U': return flags | WordEntry.VERB_TYPE_LIUL;
      case 'L': return flags | WordEntry.VERB_TYPE_LOO;
      case 'S': return flags | WordEntry.VERB_TYPE_SIUT;
      case 'D': return flags | WordEntry.VERB_TYPE_DI;
      case 'R': return flags | WordEntry.VERB_TYPE_RU;
      case 'X': return flags | WordEntry.VERB_TYPE_REGULAR;
      default: throw new IllegalArgumentException("Invalid flags: " + buffer);
    }
  }
  
  private static boolean parseBoolean(String buffer, int position) {
    if (buffer.charAt(position) == '1') {
      return true;
    } else if (buffer.charAt(position) == '0') {
      return false;
    } else {
      throw new IllegalArgumentException("Invalid flags: " + buffer);
    }
  }
}
