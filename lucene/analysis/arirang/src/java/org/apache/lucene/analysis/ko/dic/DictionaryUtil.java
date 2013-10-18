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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.ko.morph.CompoundEntry;
import org.apache.lucene.analysis.ko.morph.WordEntry;
import org.apache.lucene.analysis.ko.utils.Trie;

public class DictionaryUtil {
  private DictionaryUtil() {}
  
  private static final Trie<String,WordEntry> dictionary = new Trie<String, WordEntry>(true);
  
  private static final Set<String> josas = new HashSet<String>();
  
  private static final Set<String> eomis = new HashSet<String>();;
  
  private static final Set<String> prefixs = new HashSet<String>();;
  
  private static final Set<String> suffixs = new HashSet<String>();;
  
  private static final Map<String,WordEntry> uncompounds = new HashMap<String,WordEntry>();
  
  private static final Map<String, String> cjwords = new HashMap<String,String>();
  
  private static final Map<String, String> abbreviations = new HashMap<String,String>();
  
  static {  
    try {
      List<String> strList = DictionaryResources.readLines(DictionaryResources.FILE_DICTIONARY);
      strList.addAll(DictionaryResources.readLines(DictionaryResources.FILE_EXTENSION));
      List<String> compounds = DictionaryResources.readLines(DictionaryResources.FILE_COMPOUNDS); 
      List<String> abbrevs = DictionaryResources.readLines(DictionaryResources.FILE_ABBREV); 
    
      for(String str:strList) {
        String[] infos = str.split("[,]+");
        if(infos.length!=2) continue;
        infos[1] = infos[1].trim();
        if(infos[1].length()==6) infos[1] = infos[1].substring(0,5)+"000"+infos[1].substring(5);
        
        WordEntry entry = new WordEntry(infos[0].trim(),infos[1].trim().toCharArray());
        dictionary.add(entry.getWord(), entry);
      }
      
      for(String compound: compounds) 
      {    
        String[] infos = compound.split("[:]+");
        if(infos.length!=3&&infos.length!=2) continue;
        
        WordEntry entry = null;
        if(infos.length==2) 
          entry = new WordEntry(infos[0].trim(),"20000000X".toCharArray());
        else 
          entry = new WordEntry(infos[0].trim(),("200"+infos[2]+"0X").toCharArray());
        
        entry.setCompounds(compoundArrayToList(infos[1], infos[1].split("[,]+")));
        dictionary.add(entry.getWord(), entry);
      }
      
      for(String abbrev: abbrevs) 
      {    
        String[] infos = abbrev.split("[:]+");
        if(infos.length!=2) continue;      
        abbreviations.put(infos[0].trim(), infos[1].trim());
      }
      
      List<String> lines = DictionaryResources.readLines(DictionaryResources.FILE_UNCOMPOUNDS);  
      for(String compound: lines) {    
        String[] infos = compound.split("[:]+");
        if(infos.length!=2) continue;
        WordEntry entry = new WordEntry(infos[0].trim(),"90000X".toCharArray());
        entry.setCompounds(compoundArrayToList(infos[1], infos[1].split("[,]+")));
        uncompounds.put(entry.getWord(), entry);
      }      
  
      lines = DictionaryResources.readLines(DictionaryResources.FILE_CJ);  
      for(String cj: lines) {    
        String[] infos = cj.split("[:]+");
        if(infos.length!=2) continue;
        cjwords.put(infos[0], infos[1]);
      }
      
      readFileToSet(josas,DictionaryResources.FILE_JOSA);
      
      readFileToSet(eomis,DictionaryResources.FILE_EOMI);
      
      readFileToSet(prefixs,DictionaryResources.FILE_PREFIX);
  
      readFileToSet(suffixs,DictionaryResources.FILE_SUFFIX);
      
    } catch (IOException e) {      
      new Error("Cannot load resource",e);
    }
  }

  @SuppressWarnings({"rawtypes","unchecked"})
  public static Iterator<String[]> findWithPrefix(String prefix) {
    return dictionary.getPrefixedBy(prefix);
  }

  public static WordEntry getWord(String key) {    
    if(key.length()==0) return null;
    
    return (WordEntry)dictionary.get(key);
  }
  
  public static WordEntry getWordExceptVerb(String key) {    
    WordEntry entry = getWord(key);    
    if(entry==null) return null;
    
    if(entry.getFeature(WordEntry.IDX_NOUN)=='1'||
        entry.getFeature(WordEntry.IDX_NOUN)=='2'||
        entry.getFeature(WordEntry.IDX_BUSA)=='1'
        ) 
      return entry;
    
    return null;
  }
  
  public static WordEntry getNoun(String key) {  

    WordEntry entry = getWord(key);
    if(entry==null) return null;
    
    if(entry.getFeature(WordEntry.IDX_NOUN)=='1') return entry;
    return null;
  }
  
  /**
   * 
   * return all noun including compound noun
   * @param key the lookup key text
   * @return  WordEntry
   */
  public static WordEntry getAllNoun(String key) {  

    WordEntry entry = getWord(key);
    if(entry==null) return null;

    if(entry.getFeature(WordEntry.IDX_NOUN)=='1' || entry.getFeature(WordEntry.IDX_NOUN)=='2') return entry;
    return null;
  }
  
  public static WordEntry getVerb(String key) {
    
    WordEntry entry = getWord(key);  
    if(entry==null) return null;

    if(entry.getFeature(WordEntry.IDX_VERB)=='1') {
      return entry;
    }
    return null;
  }
  
  public static WordEntry getAdverb(String key) {
    WordEntry entry = getWord(key);
    if(entry==null) return null;

    if(entry.getFeature(WordEntry.IDX_BUSA)=='1') return entry;
    return null;
  }
  
  public static WordEntry getBusa(String key) {
    WordEntry entry = getWord(key);
    if(entry==null) return null;

    if(entry.getFeature(WordEntry.IDX_BUSA)=='1'&&entry.getFeature(WordEntry.IDX_NOUN)=='0') return entry;
    return null;
  }
  
  public static WordEntry getIrrVerb(String key, char irrType) {
    WordEntry entry = getWord(key);
    if(entry==null) return null;

    if(entry.getFeature(WordEntry.IDX_VERB)=='1'&&
        entry.getFeature(WordEntry.IDX_REGURA)==irrType) return entry;
    return null;
  }
  
  public static WordEntry getBeVerb(String key) {
    WordEntry entry = getWord(key);
    if(entry==null) return null;
    
    if(entry.getFeature(WordEntry.IDX_BEV)=='1') return entry;
    return null;
  }
  
  public static WordEntry getDoVerb(String key) {
    WordEntry entry = getWord(key);
    if(entry==null) return null;
    
    if(entry.getFeature(WordEntry.IDX_DOV)=='1') return entry;
    return null;
  }
  
  public static String getAbbrevMorph(String key) {
    return abbreviations.get(key);
  }
  
  public static WordEntry getUncompound(String key) {
    return uncompounds.get(key);
  }
  
  public static String getCJWord(String key) {
    return cjwords.get(key);
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
  
  private static void readFileToSet(Set<String> set, String dic) throws IOException {    
    List<String> line = DictionaryResources.readLines(dic);
    for(int i=1;i<line.size();i++) {
      set.add(line.get(i).trim());
    }
  }
  
  private static List<CompoundEntry> compoundArrayToList(String source, String[] arr) {
    List<CompoundEntry> list = new ArrayList<CompoundEntry>();
    for(String str: arr) {
      CompoundEntry ce = new CompoundEntry(str);
      ce.setOffset(source.indexOf(str));
      list.add(ce);
    }
    return list;
  }
}
