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
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.ko.morph.CompoundEntry;
import org.apache.lucene.analysis.ko.morph.MorphException;
import org.apache.lucene.analysis.ko.morph.WordEntry;
import org.apache.lucene.analysis.ko.utils.Trie;

public class DictionaryUtil {
  private DictionaryUtil() {}
  
  private static Trie<String,WordEntry> dictionary;
  
  private static HashMap<String, String> josas;
  
  private static HashMap<String, String> eomis;
  
  private static HashMap<String, String> prefixs;
  
  private static HashMap<String, String> suffixs;
  
  private static HashMap<String,WordEntry> uncompounds;
  
  private static HashMap<String, String> cjwords;
  
  private static HashMap<String, String> abbreviations;
  
  /**
   * 사전을 로드한다.
   */
  public synchronized static void loadDictionary() throws MorphException {
    
    dictionary = new Trie<String, WordEntry>(true);
    List<String> strList = null;
    List<String> compounds = null;
    List<String> abbrevs = null;
    try {
      strList = DictionaryResources.readLines(DictionaryResources.FILE_DICTIONARY);
      strList.addAll(DictionaryResources.readLines(DictionaryResources.FILE_EXTENSION));
      compounds = DictionaryResources.readLines(DictionaryResources.FILE_COMPOUNDS); 
      abbrevs = DictionaryResources.readLines(DictionaryResources.FILE_ABBREV); 
    } catch (IOException e) {      
      new MorphException(e.getMessage(),e);
    } catch (Exception e) {
      new MorphException(e.getMessage(),e);
    }
    if(strList==null) throw new MorphException("dictionary is null");;
    
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
    
    abbreviations = new HashMap<String, String>();
    
    for(String abbrev: abbrevs) 
    {    
      String[] infos = abbrev.split("[:]+");
      if(infos.length!=2) continue;      
      abbreviations.put(infos[0].trim(), infos[1].trim());
    }
    
  }

  @SuppressWarnings({"rawtypes","unchecked"})
  public static Iterator<String[]> findWithPrefix(String prefix) throws MorphException {
    if(dictionary==null) loadDictionary();
    return dictionary.getPrefixedBy(prefix);
  }

  public static WordEntry getWord(String key) throws MorphException {    
    if(dictionary==null) loadDictionary();
    if(key.length()==0) return null;
    
    return (WordEntry)dictionary.get(key);
  }
  
  public static WordEntry getWordExceptVerb(String key) throws MorphException {    
    WordEntry entry = getWord(key);    
    if(entry==null) return null;
    
    if(entry.getFeature(WordEntry.IDX_NOUN)=='1'||
        entry.getFeature(WordEntry.IDX_NOUN)=='2'||
        entry.getFeature(WordEntry.IDX_BUSA)=='1'
        ) 
      return entry;
    
    return null;
  }
  
  public static WordEntry getNoun(String key) throws MorphException {  

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
   * @throws MorphException throw exception
   */
  public static WordEntry getAllNoun(String key) throws MorphException {  

    WordEntry entry = getWord(key);
    if(entry==null) return null;

    if(entry.getFeature(WordEntry.IDX_NOUN)=='1' || entry.getFeature(WordEntry.IDX_NOUN)=='2') return entry;
    return null;
  }
  
  public static WordEntry getVerb(String key) throws MorphException {
    
    WordEntry entry = getWord(key);  
    if(entry==null) return null;

    if(entry.getFeature(WordEntry.IDX_VERB)=='1') {
      return entry;
    }
    return null;
  }
  
  public static WordEntry getAdverb(String key) throws MorphException {
    WordEntry entry = getWord(key);
    if(entry==null) return null;

    if(entry.getFeature(WordEntry.IDX_BUSA)=='1') return entry;
    return null;
  }
  
  public static WordEntry getBusa(String key) throws MorphException {
    WordEntry entry = getWord(key);
    if(entry==null) return null;

    if(entry.getFeature(WordEntry.IDX_BUSA)=='1'&&entry.getFeature(WordEntry.IDX_NOUN)=='0') return entry;
    return null;
  }
  
  public static WordEntry getIrrVerb(String key, char irrType) throws MorphException {
    WordEntry entry = getWord(key);
    if(entry==null) return null;

    if(entry.getFeature(WordEntry.IDX_VERB)=='1'&&
        entry.getFeature(WordEntry.IDX_REGURA)==irrType) return entry;
    return null;
  }
  
  public static WordEntry getBeVerb(String key) throws MorphException {
    WordEntry entry = getWord(key);
    if(entry==null) return null;
    
    if(entry.getFeature(WordEntry.IDX_BEV)=='1') return entry;
    return null;
  }
  
  public static WordEntry getDoVerb(String key) throws MorphException {
    WordEntry entry = getWord(key);
    if(entry==null) return null;
    
    if(entry.getFeature(WordEntry.IDX_DOV)=='1') return entry;
    return null;
  }
  
  public static String getAbbrevMorph(String key) throws MorphException {
    if(abbreviations==null) loadDictionary();    
    return abbreviations.get(key);
  }
  
  public synchronized static WordEntry getUncompound(String key) throws MorphException {
    
    try {
      if(uncompounds==null) {
        uncompounds = new HashMap<String,WordEntry>();
        List<String> lines = DictionaryResources.readLines(DictionaryResources.FILE_UNCOMPOUNDS);  
        for(String compound: lines) {    
          String[] infos = compound.split("[:]+");
          if(infos.length!=2) continue;
          WordEntry entry = new WordEntry(infos[0].trim(),"90000X".toCharArray());
          entry.setCompounds(compoundArrayToList(infos[1], infos[1].split("[,]+")));
          uncompounds.put(entry.getWord(), entry);
        }      
      }  
    }catch(Exception e) {
      throw new MorphException(e);
    }
    return uncompounds.get(key);
  }
  
  public synchronized static String getCJWord(String key) throws MorphException {
    
    try {
      if(cjwords==null) {
        cjwords = new HashMap<String, String>();
        List<String> lines = DictionaryResources.readLines(DictionaryResources.FILE_CJ);  
        for(String cj: lines) {    
          String[] infos = cj.split("[:]+");
          if(infos.length!=2) continue;
          cjwords.put(infos[0], infos[1]);
        }      
      }  
    }catch(Exception e) {
      throw new MorphException(e);
    }
    return cjwords.get(key);
    
  }
  
  public static boolean existJosa(String str) throws MorphException {
    if(josas==null) {
      josas = new HashMap<String, String>();
      readFile(josas,DictionaryResources.FILE_JOSA);
    }  
    if(josas.get(str)==null) return false;
    else return true;
  }
  
  public static boolean existEomi(String str)  throws MorphException {
    if(eomis==null) {
      eomis = new HashMap<String, String>();
      readFile(eomis,DictionaryResources.FILE_EOMI);
    }

    if(eomis.get(str)==null) return false;
    else return true;
  }
  
  public static boolean existPrefix(String str)  throws MorphException {
    if(prefixs==null) {
      prefixs = new HashMap<String, String>();
      readFile(prefixs,DictionaryResources.FILE_PREFIX);
    }

    if(prefixs.get(str)==null) return false;
    else return true;
  }
  
  public static boolean existSuffix(String str)  throws MorphException {
    if(suffixs==null) {
      suffixs = new HashMap<String, String>();
      readFile(suffixs,DictionaryResources.FILE_SUFFIX);
    }

    if(suffixs.get(str)!=null) return true;
    
    return false;
  }
  
  /**
   * ㄴ,ㄹ,ㅁ,ㅂ과 eomi 가 결합하여 어미가 될 수 있는지 점검한다.
   */
  public static String combineAndEomiCheck(char s, String eomi) throws MorphException {
  
    if(eomi==null) eomi="";

    if(s=='ㄴ') eomi = "은"+eomi;
    else if(s=='ㄹ') eomi = "을"+eomi;
    else if(s=='ㅁ') eomi = "음"+eomi;
    else if(s=='ㅂ') eomi = "습"+eomi;
    else eomi = s+eomi;

    if(existEomi(eomi)) return eomi;    

    return null;
    
  }
  
  /**
   * 
   * @param map map
   * @param dic  1: josa, 2: eomi
   * @throws MorphException excepton
   */
  private static synchronized void readFile(HashMap<String, String> map, String dic) throws MorphException {    
    try{
      List<String> line = DictionaryResources.readLines(dic);
      for(int i=1;i<line.size();i++) {
        map.put(line.get(i).trim(), line.get(i));
      }
    }catch(IOException e) {
      throw new MorphException(e.getMessage(),e);
    } catch (Exception e) {
      throw new MorphException(e.getMessage(),e);
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
