package org.apache.lucene.analysis.kuromoji.util;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.analysis.kuromoji.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.kuromoji.util.DictionaryBuilder.DictionaryFormat;


/**
 * @author Masaru Hasegawa
 * @author Christian Moen
 */
public class TokenInfoDictionaryBuilder {
  
  /** Internal word id - incrementally assigned as entries are read and added. This will be byte offset of dictionary file */
  private int offset = 4; // Start from 4. First 4 bytes are used to store size of dictionary file.
  
  private TreeMap<Integer, String> dictionaryEntries; // wordId, surface form
  
  private String encoding = "euc-jp";
  
  private boolean normalizeEntries = false;
  
  private DictionaryFormat format = DictionaryFormat.IPADIC;
  
  public TokenInfoDictionaryBuilder() {
  }
  
  public TokenInfoDictionaryBuilder(DictionaryFormat format, String encoding, boolean normalizeEntries) {
    this.format = format;
    this.encoding = encoding;
    this.dictionaryEntries = new TreeMap<Integer, String>();		
    this.normalizeEntries = normalizeEntries;
  }
  
  public TokenInfoDictionary build(String dirname) throws IOException {
    FilenameFilter filter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".csv");
      }
    };
    ArrayList<File> csvFiles = new ArrayList<File>();
    for (File file : new File(dirname).listFiles(filter)) {
      csvFiles.add(file);
    }
    return buildDictionary(csvFiles);
  }
  
  public TokenInfoDictionary buildDictionary(List<File> csvFiles) throws IOException {
    TokenInfoDictionary dictionary = new TokenInfoDictionary(10 * 1024 * 1024);
    
    for (File file : csvFiles){
      FileInputStream inputStream = new FileInputStream(file);
      InputStreamReader streamReader = new InputStreamReader(inputStream, encoding);
      BufferedReader reader = new BufferedReader(streamReader);
      
      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] entry = CSVUtil.parse(line);
        if(entry.length < 13) {
          System.out.println("Entry in CSV is not valid: " + line);
          continue;
        }
        int next = dictionary.put(formatEntry(entry));
        
        if(next == offset){
          System.out.println("Failed to process line: " + line);
          continue;
        }
        
        dictionaryEntries.put(offset, entry[0]);
        offset = next;
        
        // NFKC normalize dictionary entry
        if (normalizeEntries) {
          if (entry[0].equals(Normalizer.normalize(entry[0], Normalizer.Form.NFKC))){
            continue;
          }
          String[] normalizedEntry = new String[entry.length];
          for (int i = 0; i < entry.length; i++) {
            normalizedEntry[i] = Normalizer.normalize(entry[i], Normalizer.Form.NFKC);
          }
          
          next = dictionary.put(formatEntry(normalizedEntry));
          dictionaryEntries.put(offset, normalizedEntry[0]);
          offset = next;
        }
      }
    }
    
    return dictionary;
  }
  
  /*
   * IPADIC features
   * 
   * 0	- surface
   * 1	- left cost
   * 2	- right cost
   * 3	- word cost
   * 4-9	- pos
   * 10	- base form
   * 11	- reading
   * 12	- pronounciation
   *
   * UniDic features
   * 
   * 0	- surface
   * 1	- left cost
   * 2	- right cost
   * 3	- word cost
   * 4-9	- pos
   * 10	- base form reading
   * 11	- base form
   * 12	- surface form
   * 13	- surface reading
   */
  public String[] formatEntry(String[] features) {
    if (this.format == DictionaryFormat.IPADIC) {
      return features;
    } else {
      String[] features2 = new String[13];
      features2[0] = features[0];
      features2[1] = features[1];
      features2[2] = features[2];
      features2[3] = features[3];
      features2[4] = features[4];
      features2[5] = features[5];
      features2[6] = features[6];
      features2[7] = features[7];
      features2[8] = features[8];
      features2[9] = features[9];
      features2[10] = features[11];
      
      // If the surface reading is non-existent, use surface form for reading and pronunciation.
      // This happens with punctuation in UniDic and there are possibly other cases as well
      if (features[13].length() == 0) {
        features2[11] = features[0];
        features2[12] = features[0];
      } else {
        features2[11] = features[13];
        features2[12] = features[13];
      }			
      return features2;
    }
  }
  
  public Set<Entry<Integer, String>> entrySet() {
    return dictionaryEntries.entrySet();
  }	
}
