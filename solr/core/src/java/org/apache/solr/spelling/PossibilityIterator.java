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
package org.apache.solr.spelling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * <p>
 * Given a list of possible Spelling Corrections for multiple mis-spelled words
 * in a query, This iterator returns Possible Correction combinations ordered by
 * reasonable probability that such a combination will return actual hits if
 * re-queried. This implementation simply ranks the Possible Combinations by the
 * sum of their component ranks.
 * </p>
 * 
 */
public class PossibilityIterator implements
    Iterator<PossibilityIterator.RankedSpellPossibility> {
  private List<List<SpellCheckCorrection>> possibilityList = new ArrayList<>();
  private Iterator<RankedSpellPossibility> rankedPossibilityIterator = null;
  private int correctionIndex[];
  private boolean done = false;
  private Iterator<List<SpellCheckCorrection>> nextOnes = null;
  private int nextOnesRank = 0;
  private int nextOnesIndex = 0;
  private boolean suggestionsMayOverlap = false;
  
  @SuppressWarnings("unused")
  private PossibilityIterator() {
    throw new AssertionError("You shan't go here.");
  }
  
  /**
   * <p>
   * We assume here that the passed-in inner LinkedHashMaps are already sorted
   * in order of "Best Possible Correction".
   * </p>
   */
  public PossibilityIterator(
      Map<Token,LinkedHashMap<String,Integer>> suggestions,
      int maximumRequiredSuggestions, int maxEvaluations, boolean overlap) {
    this.suggestionsMayOverlap = overlap;
    for (Map.Entry<Token,LinkedHashMap<String,Integer>> entry : suggestions
        .entrySet()) {
      Token token = entry.getKey();
      if (entry.getValue().size() == 0) {
        continue;
      }
      List<SpellCheckCorrection> possibleCorrections = new ArrayList<>();
      for (Map.Entry<String,Integer> entry1 : entry.getValue().entrySet()) {
        SpellCheckCorrection correction = new SpellCheckCorrection();
        correction.setOriginal(token);
        correction.setCorrection(entry1.getKey());
        correction.setNumberOfOccurences(entry1.getValue());
        possibleCorrections.add(correction);
      }
      possibilityList.add(possibleCorrections);
    }
    
    int wrapSize = possibilityList.size();
    if (wrapSize == 0) {
      done = true;
    } else {
      correctionIndex = new int[wrapSize];
      for (int i = 0; i < wrapSize; i++) {
        int suggestSize = possibilityList.get(i).size();
        if (suggestSize == 0) {
          done = true;
          break;
        }
        correctionIndex[i] = 0;
      }
    }
    PriorityQueue<RankedSpellPossibility> rankedPossibilities = new PriorityQueue<>(
        11, new RankComparator());
    Set<RankedSpellPossibility> removeDuplicates = null;
    if (suggestionsMayOverlap) {
      removeDuplicates = new HashSet<>();
    }
    long numEvaluations = 0;
    while (numEvaluations < maxEvaluations && internalHasNext()) {
      RankedSpellPossibility rsp = internalNext();
      numEvaluations++;
      if (rankedPossibilities.size() >= maximumRequiredSuggestions
          && rsp.rank >= rankedPossibilities.peek().rank) {
        continue;
      }
      if (!isSuggestionForReal(rsp)) {
        continue;
      }
      if (removeDuplicates == null) {
        rankedPossibilities.offer(rsp);
      } else {
        // Needs to be in token-offset order so that the match-and-replace
        // option for collations can work.
        Collections.sort(rsp.corrections, new StartOffsetComparator());
        if (removeDuplicates.add(rsp)) {
          rankedPossibilities.offer(rsp);
        }
      }
      if (rankedPossibilities.size() > maximumRequiredSuggestions) {
        RankedSpellPossibility removed = rankedPossibilities.poll();
        if (removeDuplicates != null) {
          removeDuplicates.remove(removed);
        }
      }
    }
    
    RankedSpellPossibility[] rpArr = new RankedSpellPossibility[rankedPossibilities
        .size()];
    for (int i = rankedPossibilities.size() - 1; i >= 0; i--) {
      rpArr[i] = rankedPossibilities.remove();
    }
    rankedPossibilityIterator = Arrays.asList(rpArr).iterator();
  }
  
  private boolean isSuggestionForReal(RankedSpellPossibility rsp) {
    for (SpellCheckCorrection corr : rsp.corrections) {
      if (!corr.getOriginalAsString().equals(corr.getCorrection())) {
        return true;
      }
    }
    return false;
  }
  
  private boolean internalHasNext() {
    if (nextOnes != null && nextOnes.hasNext()) {
      return true;
    }
    if (done) {
      return false;
    }
    internalNextAdvance();
    if (nextOnes != null && nextOnes.hasNext()) {
      return true;
    }
    return false;
  }
  
  /**
   * <p>
   * This method is converting the independent LinkHashMaps containing various
   * (silo'ed) suggestions for each mis-spelled word into individual
   * "holistic query corrections", aka. "Spell Check Possibility"
   * </p>
   * <p>
   * Rank here is the sum of each selected term's position in its respective
   * LinkedHashMap.
   * </p>
   */
  private RankedSpellPossibility internalNext() {
    if (nextOnes != null && nextOnes.hasNext()) {
      RankedSpellPossibility rsl = new RankedSpellPossibility();
      rsl.corrections = nextOnes.next();
      rsl.rank = nextOnesRank;
      rsl.index = nextOnesIndex++;
      return rsl;
    }
    if (done) {
      throw new NoSuchElementException();
    }
    internalNextAdvance();
    if (nextOnes != null && nextOnes.hasNext()) {
      RankedSpellPossibility rsl = new RankedSpellPossibility();
      rsl.corrections = nextOnes.next();
      rsl.rank = nextOnesRank;
      rsl.index = nextOnesIndex++;
      return rsl;
    }
    throw new NoSuchElementException();
  }
  
  private void internalNextAdvance() {
    List<SpellCheckCorrection> possibleCorrection = null;
    if (nextOnes != null && nextOnes.hasNext()) {
      possibleCorrection = nextOnes.next();
    } else {
      if (done) {
        throw new NoSuchElementException();
      }
      possibleCorrection = new ArrayList<>();
      List<List<SpellCheckCorrection>> possibleCorrections = null;
      int rank = 0;
      while (!done
          && (possibleCorrections == null || possibleCorrections.size() == 0)) {
        rank = 0;
        for (int i = 0; i < correctionIndex.length; i++) {
          List<SpellCheckCorrection> singleWordPossibilities = possibilityList
              .get(i);
          SpellCheckCorrection singleWordPossibility = singleWordPossibilities
              .get(correctionIndex[i]);
          rank += correctionIndex[i];
          if (i == correctionIndex.length - 1) {
            correctionIndex[i]++;
            if (correctionIndex[i] == singleWordPossibilities.size()) {
              correctionIndex[i] = 0;
              if (correctionIndex.length == 1) {
                done = true;
              }
              for (int ii = i - 1; ii >= 0; ii--) {
                correctionIndex[ii]++;
                if (correctionIndex[ii] >= possibilityList.get(ii).size()
                    && ii > 0) {
                  correctionIndex[ii] = 0;
                } else {
                  break;
                }
              }
            }
          }
          possibleCorrection.add(singleWordPossibility);
        }
        if (correctionIndex[0] == possibilityList.get(0).size()) {
          done = true;
        }
        if (suggestionsMayOverlap) {
          possibleCorrections = separateOverlappingTokens(possibleCorrection);
        } else {
          possibleCorrections = new ArrayList<>(1);
          possibleCorrections.add(possibleCorrection);
        }
      }
      nextOnes = possibleCorrections.iterator();
      nextOnesRank = rank;
      nextOnesIndex = 0;
    }
  }
  
  private List<List<SpellCheckCorrection>> separateOverlappingTokens(
      List<SpellCheckCorrection> possibleCorrection) {
    List<List<SpellCheckCorrection>> ret = null;
    if (possibleCorrection.size() == 1) {
      ret = new ArrayList<>(1);
      ret.add(possibleCorrection);
      return ret;
    }
    ret = new ArrayList<>();
    for (int i = 0; i < possibleCorrection.size(); i++) {
      List<SpellCheckCorrection> c = compatible(possibleCorrection, i);
      ret.add(c);
    }
    return ret;
  }
  
  private List<SpellCheckCorrection> compatible(List<SpellCheckCorrection> all,
      int pos) {
    List<SpellCheckCorrection> priorPassCompatibles = null;
    {
      List<SpellCheckCorrection> firstPassCompatibles = new ArrayList<>(
          all.size());
      SpellCheckCorrection sacred = all.get(pos);
      firstPassCompatibles.add(sacred);
      int index = pos;
      boolean gotOne = false;
      for (int i = 0; i < all.size() - 1; i++) {
        index++;
        if (index == all.size()) {
          index = 0;
        }
        SpellCheckCorrection disposable = all.get(index);
        if (!conflicts(sacred, disposable)) {
          firstPassCompatibles.add(disposable);
          gotOne = true;
        }
      }
      if (!gotOne) {
        return firstPassCompatibles;
      }
      priorPassCompatibles = firstPassCompatibles;
    }
    
    {
      pos = 1;
      while (true) {
        if (pos == priorPassCompatibles.size() - 1) {
          return priorPassCompatibles;
        }
        List<SpellCheckCorrection> subsequentPassCompatibles = new ArrayList<>(
            priorPassCompatibles.size());
        SpellCheckCorrection sacred = null;
        for (int i = 0; i <= pos; i++) {
          sacred = priorPassCompatibles.get(i);
          subsequentPassCompatibles.add(sacred);
        }
        int index = pos;
        boolean gotOne = false;
        for (int i = 0; i < priorPassCompatibles.size() - 1; i++) {
          index++;
          if (index == priorPassCompatibles.size()) {
            break;
          }
          SpellCheckCorrection disposable = priorPassCompatibles.get(index);
          if (!conflicts(sacred, disposable)) {
            subsequentPassCompatibles.add(disposable);
            gotOne = true;
          }
        }
        if (!gotOne || pos == priorPassCompatibles.size() - 1) {
          return subsequentPassCompatibles;
        }
        priorPassCompatibles = subsequentPassCompatibles;
        pos++;
      }
    }
  }
  
  private boolean conflicts(SpellCheckCorrection c1, SpellCheckCorrection c2) {
    int s1 = c1.getOriginal().startOffset();
    int e1 = c1.getOriginal().endOffset();
    int s2 = c2.getOriginal().startOffset();
    int e2 = c2.getOriginal().endOffset();
    if (s2 >= s1 && s2 <= e1) {
      return true;
    }
    if (s1 >= s2 && s1 <= e2) {
      return true;
    }
    return false;
  }
  
  @Override
  public boolean hasNext() {
    return rankedPossibilityIterator.hasNext();
  }
  
  @Override
  public PossibilityIterator.RankedSpellPossibility next() {
    return rankedPossibilityIterator.next();
  }
  
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
  
  public static class RankedSpellPossibility {
    public List<SpellCheckCorrection> corrections;
    public int rank;
    public int index;
    
    @Override
    // hashCode() and equals() only consider the actual correction, not the rank
    // or index.
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((corrections == null) ? 0 : corrections.hashCode());
      return result;
    }
    
    @Override
    // hashCode() and equals() only consider the actual correction, not the rank
    // or index.
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      RankedSpellPossibility other = (RankedSpellPossibility) obj;
      if (corrections == null) {
        if (other.corrections != null) return false;
      } else if (!corrections.equals(other.corrections)) return false;
      return true;
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("rank=").append(rank).append(" (").append(index).append(")");
      if (corrections != null) {
        for (SpellCheckCorrection corr : corrections) {
          sb.append("     ");
          sb.append(corr.getOriginal()).append(">")
              .append(corr.getCorrection()).append(" (").append(
                  corr.getNumberOfOccurences()).append(")");
        }
      }
      return sb.toString();
    }
  }
  
  private static class StartOffsetComparator implements
      Comparator<SpellCheckCorrection> {
    @Override
    public int compare(SpellCheckCorrection o1, SpellCheckCorrection o2) {
      return o1.getOriginal().startOffset() - o2.getOriginal().startOffset();
    }
  }
  
  private static class RankComparator implements Comparator<RankedSpellPossibility> {
    // Rank poorer suggestions ahead of better ones for use with a PriorityQueue
    @Override
    public int compare(RankedSpellPossibility r1, RankedSpellPossibility r2) {
      int retval = r2.rank - r1.rank;
      if (retval == 0) {
        retval = r2.index - r1.index;
      }
      return retval;
    }
  }
  
}
