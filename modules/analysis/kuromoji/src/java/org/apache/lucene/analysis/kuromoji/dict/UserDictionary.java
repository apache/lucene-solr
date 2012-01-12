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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.analysis.kuromoji.util.CSVUtil;

public final class UserDictionary implements Dictionary {
  
  private final TreeMap<String, int[]> entries = new TreeMap<String, int[]>();
  
  private final String featureEntries[];
  
  private static final int CUSTOM_DICTIONARY_WORD_ID_OFFSET = 100000000;
  
  public static final int WORD_COST = -100000;
  
  public static final int LEFT_ID = 5;
  
  public static final int RIGHT_ID = 5;
  
  public UserDictionary(Reader reader) throws IOException {
    BufferedReader br = new BufferedReader(reader);
    String line = null;
    int wordId = CUSTOM_DICTIONARY_WORD_ID_OFFSET;
    List<String> featureEntries = new ArrayList<String>();
    while ((line = br.readLine()) != null) {
      // Remove comments
      line = line.replaceAll("#.*$", "");
      
      // Skip empty lines or comment lines
      if (line.trim().length() == 0) {
        continue;
      }
      String[] values = CSVUtil.parse(line);
      String[] segmentation = values[1].replaceAll("  *", " ").split(" ");
      String[] readings = values[2].replaceAll("  *", " ").split(" ");
      String pos = values[3];
      
      if (segmentation.length != readings.length) {
        // FIXME: Should probably deal with this differently.  Exception?
        System.out.println("This entry is not properly formatted : " + line);
      }
      
      int[] wordIdAndLength = new int[segmentation.length + 1]; // wordId offset, length, length....
      wordIdAndLength[0] = wordId;
      for (int i = 0; i < segmentation.length; i++) {
        wordIdAndLength[i + 1] = segmentation[i].length();
        featureEntries.add(readings[i] + INTERNAL_SEPARATOR + pos);
        wordId++;
      }
      entries.put(values[0], wordIdAndLength);
    }
    this.featureEntries = featureEntries.toArray(new String[featureEntries.size()]);
  }
  
  /**
   * Lookup words in text
   * @param chars text
   * @param off offset into text
   * @param len length of text
   * @return array of {wordId, position, length}
   */
  public int[][] lookup(char[] chars, int off, int len) {
    // TODO: this method should be more efficient.
    String text = new String(chars, off, len);
    TreeMap<Integer, int[]> result = new TreeMap<Integer, int[]>(); // index, [length, length...]
    
    for (String keyword : entries.descendingKeySet()) {
      int offset = 0;
      int position = text.indexOf(keyword, offset);
      while (offset < text.length() && position >= 0) {
        if(!result.containsKey(position)){
          result.put(position, entries.get(keyword));
        }
        offset += position + keyword.length();
        position = text.indexOf(keyword, offset);
      }
    }
    
    return toIndexArray(result);
  }
  
  /**
   * Convert Map of index and wordIdAndLength to array of {wordId, index, length}
   * @param input
   * @return array of {wordId, index, length}
   */
  private int[][] toIndexArray(Map<Integer, int[]> input) {
    ArrayList<int[]> result = new ArrayList<int[]>();
    for (int i : input.keySet()) {
      int[] wordIdAndLength = input.get(i);
      int wordId = wordIdAndLength[0];
      // convert length to index
      int current = i;
      for (int j = 1; j < wordIdAndLength.length; j++) { // first entry is wordId offset
        int[] token = { wordId + j - 1, current, wordIdAndLength[j] };
        result.add(token);
        current += wordIdAndLength[j];
      }
    }
    return result.toArray(new int[result.size()][]);
  }
  
  @Override
  public int getLeftId(int wordId) {
    return LEFT_ID;
  }
  
  @Override
  public int getRightId(int wordId) {
    return RIGHT_ID;
  }
  
  @Override
  public int getWordCost(int wordId) {
    return WORD_COST;
  }
  
  @Override
  public String getReading(int wordId) {
    return getFeature(wordId, 0);
  }
  
  @Override
  public String getPartOfSpeech(int wordId) {
    return getFeature(wordId, 1);
  }
  
  @Override
  public String getBaseForm(int wordId) {
    return null; // TODO: add support?
  }
  
  @Override
  public String getPronunciation(int wordId) {
    return null; // TODO: add support?
  }
  
  @Override
  public String getInflectionType(int wordId) {
    return null; // TODO: add support?
  }

  @Override
  public String getInflectionForm(int wordId) {
    return null; // TODO: add support?
  }
  
  private String[] getAllFeaturesArray(int wordId) {
    String allFeatures = featureEntries[wordId-CUSTOM_DICTIONARY_WORD_ID_OFFSET];
    if(allFeatures == null) {
      return null;
    }
    
    return allFeatures.split(INTERNAL_SEPARATOR);		
  }
  
  
  private String getFeature(int wordId, int... fields) {
    String[] allFeatures = getAllFeaturesArray(wordId);
    if (allFeatures == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    if (fields.length == 0) { // All features
      for (String feature : allFeatures) {
        sb.append(CSVUtil.quoteEscape(feature)).append(",");
      }
    } else if (fields.length == 1) { // One feature doesn't need to escape value
      sb.append(allFeatures[fields[0]]).append(",");			
    } else {
      for (int field : fields){
        sb.append(CSVUtil.quoteEscape(allFeatures[field])).append(",");
      }
    }
    return sb.deleteCharAt(sb.length() - 1).toString();
  }
  
}
