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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.analysis.ja.util.CSVUtil;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;

/**
 * Class for building a User Dictionary.
 * This class allows for custom segmentation of phrases.
 */
public final class UserDictionary implements Dictionary {
  
  // phrase text -> phrase ID
  private final TokenInfoFST fst;
  
  // holds wordid, length, length... indexed by phrase ID
  private final int segmentations[][];
  
  // holds readings and POS, indexed by wordid
  private final String data[];
  
  private static final int CUSTOM_DICTIONARY_WORD_ID_OFFSET = 100000000;
  
  public static final int WORD_COST = -100000;
  
  public static final int LEFT_ID = 5;
  
  public static final int RIGHT_ID = 5;

  public static UserDictionary open(Reader reader) throws IOException {

    BufferedReader br = new BufferedReader(reader);
    String line = null;
    List<String[]> featureEntries = new ArrayList<>();

    // text, segmentation, readings, POS
    while ((line = br.readLine()) != null) {
      // Remove comments
      line = line.replaceAll("#.*$", "");

      // Skip empty lines or comment lines
      if (line.trim().length() == 0) {
        continue;
      }
      String[] values = CSVUtil.parse(line);
      featureEntries.add(values);
    }

    if (featureEntries.isEmpty()) {
      return null;
    } else {
      return new UserDictionary(featureEntries);
    }
  }

  private UserDictionary(List<String[]> featureEntries) throws IOException {

    int wordId = CUSTOM_DICTIONARY_WORD_ID_OFFSET;
    // TODO: should we allow multiple segmentations per input 'phrase'?
    // the old treemap didn't support this either, and i'm not sure if it's needed/useful?

    Collections.sort(featureEntries, new Comparator<String[]>() {
      @Override
      public int compare(String[] left, String[] right) {
        return left[0].compareTo(right[0]);
     }
    });
    
    List<String> data = new ArrayList<>(featureEntries.size());
    List<int[]> segmentations = new ArrayList<>(featureEntries.size());
    
    PositiveIntOutputs fstOutput = PositiveIntOutputs.getSingleton();
    Builder<Long> fstBuilder = new Builder<>(FST.INPUT_TYPE.BYTE2, fstOutput);
    IntsRefBuilder scratch = new IntsRefBuilder();
    long ord = 0;
    
    for (String[] values : featureEntries) {
      String surface = values[0].replaceAll("\\s", "");
      String concatenatedSegment = values[1].replaceAll("\\s", "");
      String[] segmentation = values[1].replaceAll("  *", " ").split(" ");
      String[] readings = values[2].replaceAll("  *", " ").split(" ");
      String pos = values[3];
      
      if (segmentation.length != readings.length) {
        throw new RuntimeException("Illegal user dictionary entry " + values[0] +
                                   " - the number of segmentations (" + segmentation.length + ")" +
                                   " does not the match number of readings (" + readings.length + ")");
      }

      if (concatenatedSegment.length() > surface.length()) {
        throw new RuntimeException("Illegal user dictionary entry " + values[0] +
            " - the concatenated segmentation (" + concatenatedSegment + ")" +
            " is longer than the surface form (" + surface + ")");
      }
      
      int[] wordIdAndLength = new int[segmentation.length + 1]; // wordId offset, length, length....
      wordIdAndLength[0] = wordId;
      for (int i = 0; i < segmentation.length; i++) {
        wordIdAndLength[i + 1] = segmentation[i].length();
        data.add(readings[i] + INTERNAL_SEPARATOR + pos);
        wordId++;
      }
      // add mapping to FST
      String token = values[0];
      scratch.grow(token.length());
      scratch.setLength(token.length());
      for (int i = 0; i < token.length(); i++) {
        scratch.setIntAt(i, (int) token.charAt(i));
      }
      fstBuilder.add(scratch.get(), ord);
      segmentations.add(wordIdAndLength);
      ord++;
    }
    this.fst = new TokenInfoFST(fstBuilder.finish(), false);
    this.data = data.toArray(new String[data.size()]);
    this.segmentations = segmentations.toArray(new int[segmentations.size()][]);
  }
  
  /**
   * Lookup words in text
   * @param chars text
   * @param off offset into text
   * @param len length of text
   * @return array of {wordId, position, length}
   */
  public int[][] lookup(char[] chars, int off, int len) throws IOException {
    // TODO: can we avoid this treemap/toIndexArray?
    TreeMap<Integer, int[]> result = new TreeMap<>(); // index, [length, length...]
    boolean found = false; // true if we found any results

    final FST.BytesReader fstReader = fst.getBytesReader();

    FST.Arc<Long> arc = new FST.Arc<>();
    int end = off + len;
    for (int startOffset = off; startOffset < end; startOffset++) {
      arc = fst.getFirstArc(arc);
      int output = 0;
      int remaining = end - startOffset;
      for (int i = 0; i < remaining; i++) {
        int ch = chars[startOffset+i];
        if (fst.findTargetArc(ch, arc, arc, i == 0, fstReader) == null) {
          break; // continue to next position
        }
        output += arc.output().intValue();
        if (arc.isFinal()) {
          final int finalOutput = output + arc.nextFinalOutput().intValue();
          result.put(startOffset-off, segmentations[finalOutput]);
          found = true;
        }
      }
    }
    
    return found ? toIndexArray(result) : EMPTY_RESULT;
  }
  
  public TokenInfoFST getFST() {
    return fst;
  }

  private static final int[][] EMPTY_RESULT = new int[0][];
  
  /**
   * Convert Map of index and wordIdAndLength to array of {wordId, index, length}
   * @return array of {wordId, index, length}
   */
  private int[][] toIndexArray(Map<Integer, int[]> input) {
    ArrayList<int[]> result = new ArrayList<>();
    for (Map.Entry<Integer, int[]> entry : input.entrySet()) {
      int[] wordIdAndLength = entry.getValue();
      int wordId = wordIdAndLength[0];
      // convert length to index
      int current = entry.getKey();
      for (int j = 1; j < wordIdAndLength.length; j++) { // first entry is wordId offset
        int[] token = { wordId + j - 1, current, wordIdAndLength[j] };
        result.add(token);
        current += wordIdAndLength[j];
      }
    }
    return result.toArray(new int[result.size()][]);
  }

  public int[] lookupSegmentation(int phraseID) {
    return segmentations[phraseID];
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
  public String getReading(int wordId, char surface[], int off, int len) {
    return getFeature(wordId, 0);
  }
  
  @Override
  public String getPartOfSpeech(int wordId) {
    return getFeature(wordId, 1);
  }
  
  @Override
  public String getBaseForm(int wordId, char surface[], int off, int len) {
    return null; // TODO: add support?
  }
  
  @Override
  public String getPronunciation(int wordId, char surface[], int off, int len) {
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
    String allFeatures = data[wordId-CUSTOM_DICTIONARY_WORD_ID_OFFSET];
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
