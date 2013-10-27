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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.FST;

public class DictionaryUtil {
  private DictionaryUtil() {}
  
  private static final HangulDictionary dictionary;
  
  private static final Set<String> josas = new HashSet<String>();
  
  private static final Set<String> eomis = new HashSet<String>();;
  
  private static final Set<String> prefixs = new HashSet<String>();;
  
  private static final Set<String> suffixs = new HashSet<String>();;
  
  private static final Set<String> uncompounds = new HashSet<String>();
  
  static {  
    try {
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
      if (stream == null)
        throw new FileNotFoundException(DictionaryResources.FILE_WORDS_DAT);
      try {
        DataInput dat = new InputStreamDataInput(new BufferedInputStream(stream));
        CodecUtil.checkHeader(dat, DictionaryResources.FILE_WORDS_DAT, DictionaryResources.DATA_VERSION, DictionaryResources.DATA_VERSION);
        byte metadata[] = new byte[dat.readByte() * HangulDictionary.RECORD_SIZE];
        dat.readBytes(metadata, 0, metadata.length);
        ByteOutputs outputs = ByteOutputs.getSingleton();
        FST<Byte> fst = new FST<Byte>(dat, outputs);
        dictionary = new HangulDictionary(fst, metadata);
      } finally {
        IOUtils.closeWhileHandlingException(stream);
      }
    } catch (IOException e) {
      throw new Error("Cannot load resource",e);
    }
  }
  
  /** true if something with this prefix exists */
  public static boolean hasWordPrefix(CharSequence prefix) {
    return dictionary.hasPrefix(prefix);
  }

  /** only use this if you surely need the whole entry */
  public static WordEntry getWord(String key) {    
    Byte clazz = dictionary.lookup(key);
    if (clazz == null) {
      return null;
    } else {
      return new WordEntry(key, dictionary.getFlags(clazz), clazz);
    }
  }
  
  /** Looks up noun, compound noun, or adverb */
  public static WordEntry getWordExceptVerb(String key) {
    Byte clazz = dictionary.lookup(key);
    if (clazz == null) {
      return null;
    }
    char flags = dictionary.getFlags(clazz);
    if ((flags & (WordEntry.NOUN | WordEntry.BUSA)) != 0) {
      return new WordEntry(key, flags, clazz);
    } else {
      return null;
    }
  }
  
  /** Looks up a noun (but not compound noun) */
  public static WordEntry getNoun(String key) {
    Byte clazz = dictionary.lookup(key);
    if (clazz == null) {
      return null;
    }
    char flags = dictionary.getFlags(clazz);
    if ((flags & WordEntry.NOUN) != 0 && (flags & WordEntry.COMPOUND) == 0) {
      return new WordEntry(key, flags, clazz);
    } else {
      return null;
    }
  }
  
  /**
   * return all noun including compound noun
   */
  public static WordEntry getAllNoun(String key) {  
    Byte clazz = dictionary.lookup(key);
    if (clazz == null) {
      return null;
    }
    char flags = dictionary.getFlags(clazz);
    if ((flags & WordEntry.NOUN) != 0) {
      return new WordEntry(key, flags, clazz);
    } else {
      return null;
    }
  }
  
  /**
   * returns any verb
   */
  public static WordEntry getVerb(String key) {
    Byte clazz = dictionary.lookup(key);
    if (clazz == null) {
      return null;
    }
    char flags = dictionary.getFlags(clazz);
    if ((flags & WordEntry.VERB) != 0) {
      return new WordEntry(key, flags, clazz);
    } else {
      return null;
    }
  }
  
  /** Looks up an adverb-only */
  public static WordEntry getBusa(String key) {
    Byte clazz = dictionary.lookup(key);
    if (clazz == null) {
      return null;
    }
    char flags = dictionary.getFlags(clazz);
    if ((flags & WordEntry.BUSA) != 0 && (flags & WordEntry.NOUN) == 0) {
      return new WordEntry(key, flags, clazz);
    } else {
      return null;
    }
  }
  
  /** return list of irregular compounds for word class. */
  static List<CompoundEntry> getIrregularCompounds(byte clazz) {
    return dictionary.getIrregularCompounds(clazz);
  }
  
  /** return list of compounds for key and word class. */
  static List<CompoundEntry> getCompounds(String key, byte clazz) {
    return dictionary.getCompounds(key, clazz);
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
  
  private static void readFileToSet(final Set<String> set, String dic) throws IOException {    
    DictionaryResources.readLines(dic, new LineProcessor() {
      @Override
      public void processLine(String line) {
        set.add(line.trim());
      }
    });
  }
}
