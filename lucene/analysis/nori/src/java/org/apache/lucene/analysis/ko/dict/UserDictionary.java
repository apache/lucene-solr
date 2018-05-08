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
package org.apache.lucene.analysis.ko.dict;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.analysis.ko.POS;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;

/**
 * Class for building a User Dictionary.
 * This class allows for adding custom nouns (세종) or compounds (세종시 세종 시).
 */
public final class UserDictionary implements Dictionary {
  // text -> wordID
  private final TokenInfoFST fst;

  public static final int WORD_COST = -100000;

  // NNG left
  public static final short LEFT_ID = 1781;

  // NNG right
  public static final short RIGHT_ID = 3534;
  // NNG right with hangul and a coda on the last char
  public static final short RIGHT_ID_T = 3534;
  // NNG right with hangul and no coda on the last char
  public static final short RIGHT_ID_F = 3535;

  // length, length... indexed by compound ID or null for simple noun
  private final int segmentations[][];
  private final short[] rightIds;

  public static UserDictionary open(Reader reader) throws IOException {

    BufferedReader br = new BufferedReader(reader);
    String line = null;
    List<String> entries = new ArrayList<>();

    // text + optional segmentations
    while ((line = br.readLine()) != null) {
      // Remove comments
      line = line.replaceAll("#.*$", "");

      // Skip empty lines or comment lines
      if (line.trim().length() == 0) {
        continue;
      }
      entries.add(line);
    }

    if (entries.isEmpty()) {
      return null;
    } else {
      return new UserDictionary(entries);
    }
  }

  private UserDictionary(List<String> entries) throws IOException {
    final CharacterDefinition charDef = CharacterDefinition.getInstance();
    Collections.sort(entries,
        Comparator.comparing(e -> e.split("\\s+")[0]));

    PositiveIntOutputs fstOutput = PositiveIntOutputs.getSingleton();
    Builder<Long> fstBuilder = new Builder<>(FST.INPUT_TYPE.BYTE2, fstOutput);
    IntsRefBuilder scratch = new IntsRefBuilder();

    String lastToken = null;
    List<int[]> segmentations = new ArrayList<>(entries.size());
    List<Short> rightIds = new ArrayList<>(entries.size());
    long ord = 0;
    for (String entry : entries) {
      String[] splits = entry.split("\\s+");
      String token = splits[0];
      if (lastToken != null && token.equals(lastToken)) {
        continue;
      }
      char lastChar = entry.charAt(entry.length()-1);
      if (charDef.isHangul(lastChar)) {
        if (charDef.hasCoda(lastChar)) {
          rightIds.add(RIGHT_ID_T);
        } else {
          rightIds.add(RIGHT_ID_F);
        }
      } else {
        rightIds.add(RIGHT_ID);
      }

      if (splits.length == 1) {
        segmentations.add(null);
      } else {
        int[] length = new int[splits.length-1];
        int offset = 0;
        for (int i = 1; i < splits.length; i++) {
          length[i-1] = splits[i].length();
          offset += splits[i].length();
        }
        if (offset > token.length()) {
          throw new IllegalArgumentException("Illegal user dictionary entry " + entry +
              " - the segmentation is bigger than the surface form (" + token + ")");
        }
        segmentations.add(length);
      }

      // add mapping to FST
      scratch.grow(token.length());
      scratch.setLength(token.length());
      for (int i = 0; i < token.length(); i++) {
        scratch.setIntAt(i, (int) token.charAt(i));
      }
      fstBuilder.add(scratch.get(), ord);
      lastToken = token;
      ord ++;
    }
    this.fst = new TokenInfoFST(fstBuilder.finish());
    this.segmentations = segmentations.toArray(new int[segmentations.size()][]);
    this.rightIds = new short[rightIds.size()];
    for (int i = 0; i < rightIds.size(); i++) {
      this.rightIds[i] = rightIds.get(i);
    }
  }

  public TokenInfoFST getFST() {
    return fst;
  }

  @Override
  public int getLeftId(int wordId) {
    return LEFT_ID;
  }
  
  @Override
  public int getRightId(int wordId) {
    return rightIds[wordId];
  }
  
  @Override
  public int getWordCost(int wordId) {
    return WORD_COST;
  }

  @Override
  public POS.Type getPOSType(int wordId) {
    if (segmentations[wordId] == null) {
      return POS.Type.MORPHEME;
    } else {
      return POS.Type.COMPOUND;
    }
  }

  @Override
  public POS.Tag getLeftPOS(int wordId) {
    return POS.Tag.NNG;
  }

  @Override
  public POS.Tag getRightPOS(int wordId) {
    return POS.Tag.NNG;
  }

  @Override
  public String getReading(int wordId) {
    return null;
  }

  @Override
  public Morpheme[] getMorphemes(int wordId, char[] surfaceForm, int off, int len) {
    int[] segs = segmentations[wordId];
    if (segs == null) {
      return null;
    }
    int offset = 0;
    Morpheme[] morphemes = new Morpheme[segs.length];
    for (int i = 0; i < segs.length; i++) {
      morphemes[i] = new Morpheme(POS.Tag.NNG, new String(surfaceForm, off+offset, segs[i]));
      offset += segs[i];
    }
    return morphemes;
  }

  /**
   * Lookup words in text
   * @param chars text
   * @param off offset into text
   * @param len length of text
   * @return array of wordId
   */
  public List<Integer> lookup(char[] chars, int off, int len) throws IOException {
    List<Integer> result = new ArrayList<>();
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
        output += arc.output.intValue();
        if (arc.isFinal()) {
          final int finalOutput = output + arc.nextFinalOutput.intValue();
          result.add(finalOutput);
        }
      }
    }
    return result;
  }
}
