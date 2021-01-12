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
package org.apache.lucene.search.vectorhighlight;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.search.vectorhighlight.FieldQuery.QueryPhraseMap;
import org.apache.lucene.search.vectorhighlight.FieldTermStack.TermInfo;
import org.apache.lucene.util.MergedIterator;

/**
 * FieldPhraseList has a list of WeightedPhraseInfo that is used by FragListBuilder to create a
 * FieldFragList object.
 */
public class FieldPhraseList {
  /** List of non-overlapping WeightedPhraseInfo objects. */
  LinkedList<WeightedPhraseInfo> phraseList = new LinkedList<>();

  /**
   * create a FieldPhraseList that has no limit on the number of phrases to analyze
   *
   * @param fieldTermStack FieldTermStack object
   * @param fieldQuery FieldQuery object
   */
  public FieldPhraseList(FieldTermStack fieldTermStack, FieldQuery fieldQuery) {
    this(fieldTermStack, fieldQuery, Integer.MAX_VALUE);
  }

  /**
   * return the list of WeightedPhraseInfo.
   *
   * @return phraseList.
   */
  public List<WeightedPhraseInfo> getPhraseList() {
    return phraseList;
  }

  /**
   * a constructor.
   *
   * @param fieldTermStack FieldTermStack object
   * @param fieldQuery FieldQuery object
   * @param phraseLimit maximum size of phraseList
   */
  public FieldPhraseList(FieldTermStack fieldTermStack, FieldQuery fieldQuery, int phraseLimit) {
    final String field = fieldTermStack.getFieldName();

    LinkedList<TermInfo> phraseCandidate = new LinkedList<>();
    QueryPhraseMap currMap = null;
    QueryPhraseMap nextMap = null;
    while (!fieldTermStack.isEmpty() && (phraseList.size() < phraseLimit)) {
      phraseCandidate.clear();

      TermInfo ti = null;
      TermInfo first = null;

      first = ti = fieldTermStack.pop();
      currMap = fieldQuery.getFieldTermMap(field, ti.getText());
      while (currMap == null && ti.getNext() != first) {
        ti = ti.getNext();
        currMap = fieldQuery.getFieldTermMap(field, ti.getText());
      }

      // if not found, discard top TermInfo from stack, then try next element
      if (currMap == null) continue;

      // if found, search the longest phrase
      phraseCandidate.add(ti);
      while (true) {
        first = ti = fieldTermStack.pop();
        nextMap = null;
        if (ti != null) {
          nextMap = currMap.getTermMap(ti.getText());
          while (nextMap == null && ti.getNext() != first) {
            ti = ti.getNext();
            nextMap = currMap.getTermMap(ti.getText());
          }
        }
        if (ti == null || nextMap == null) {
          if (ti != null) fieldTermStack.push(ti);
          if (currMap.isValidTermOrPhrase(phraseCandidate)) {
            addIfNoOverlap(
                new WeightedPhraseInfo(
                    phraseCandidate, currMap.getBoost(), currMap.getTermOrPhraseNumber()));
          } else {
            while (phraseCandidate.size() > 1) {
              fieldTermStack.push(phraseCandidate.removeLast());
              currMap = fieldQuery.searchPhrase(field, phraseCandidate);
              if (currMap != null) {
                addIfNoOverlap(
                    new WeightedPhraseInfo(
                        phraseCandidate, currMap.getBoost(), currMap.getTermOrPhraseNumber()));
                break;
              }
            }
          }
          break;
        } else {
          phraseCandidate.add(ti);
          currMap = nextMap;
        }
      }
    }
  }

  /**
   * Merging constructor.
   *
   * @param toMerge FieldPhraseLists to merge to build this one
   */
  public FieldPhraseList(FieldPhraseList[] toMerge) {
    // Merge all overlapping WeightedPhraseInfos
    // Step 1.  Sort by startOffset, endOffset, and boost, in that order.
    @SuppressWarnings({"rawtypes", "unchecked"})
    Iterator<WeightedPhraseInfo>[] allInfos = new Iterator[toMerge.length];
    int index = 0;
    for (FieldPhraseList fplToMerge : toMerge) {
      allInfos[index++] = fplToMerge.phraseList.iterator();
    }
    MergedIterator<WeightedPhraseInfo> itr = new MergedIterator<>(false, allInfos);
    // Step 2.  Walk the sorted list merging infos that overlap
    phraseList = new LinkedList<>();
    if (!itr.hasNext()) {
      return;
    }
    List<WeightedPhraseInfo> work = new ArrayList<>();
    WeightedPhraseInfo first = itr.next();
    work.add(first);
    int workEndOffset = first.getEndOffset();
    while (itr.hasNext()) {
      WeightedPhraseInfo current = itr.next();
      if (current.getStartOffset() <= workEndOffset) {
        workEndOffset = Math.max(workEndOffset, current.getEndOffset());
        work.add(current);
      } else {
        if (work.size() == 1) {
          phraseList.add(work.get(0));
          work.set(0, current);
        } else {
          phraseList.add(new WeightedPhraseInfo(work));
          work.clear();
          work.add(current);
        }
        workEndOffset = current.getEndOffset();
      }
    }
    if (work.size() == 1) {
      phraseList.add(work.get(0));
    } else {
      phraseList.add(new WeightedPhraseInfo(work));
      work.clear();
    }
  }

  public void addIfNoOverlap(WeightedPhraseInfo wpi) {
    for (WeightedPhraseInfo existWpi : getPhraseList()) {
      if (existWpi.isOffsetOverlap(wpi)) {
        // WeightedPhraseInfo.addIfNoOverlap() dumps the second part of, for example, hyphenated
        // words (social-economics).
        // The result is that all informations in TermInfo are lost and not available for further
        // operations.
        existWpi.getTermsInfos().addAll(wpi.getTermsInfos());
        return;
      }
    }
    getPhraseList().add(wpi);
  }

  /** Represents the list of term offsets and boost for some text */
  public static class WeightedPhraseInfo implements Comparable<WeightedPhraseInfo> {
    private List<Toffs> termsOffsets; // usually termsOffsets.size() == 1,
    // but if position-gap > 1 and slop > 0 then size() could be greater than 1
    private float boost; // query boost
    private int seqnum;

    private ArrayList<TermInfo> termsInfos;

    /**
     * Text of the match, calculated on the fly. Use for debugging only.
     *
     * @return the text
     */
    public String getText() {
      StringBuilder text = new StringBuilder();
      for (TermInfo ti : termsInfos) {
        text.append(ti.getText());
      }
      return text.toString();
    }

    /** @return the termsOffsets */
    public List<Toffs> getTermsOffsets() {
      return termsOffsets;
    }

    /** @return the boost */
    public float getBoost() {
      return boost;
    }

    /** @return the termInfos */
    public List<TermInfo> getTermsInfos() {
      return termsInfos;
    }

    public WeightedPhraseInfo(LinkedList<TermInfo> terms, float boost) {
      this(terms, boost, 0);
    }

    public WeightedPhraseInfo(LinkedList<TermInfo> terms, float boost, int seqnum) {
      this.boost = boost;
      this.seqnum = seqnum;

      // We keep TermInfos for further operations
      termsInfos = new ArrayList<>(terms);

      termsOffsets = new ArrayList<>(terms.size());
      TermInfo ti = terms.get(0);
      termsOffsets.add(new Toffs(ti.getStartOffset(), ti.getEndOffset()));
      if (terms.size() == 1) {
        return;
      }
      int pos = ti.getPosition();
      for (int i = 1; i < terms.size(); i++) {
        ti = terms.get(i);
        if (ti.getPosition() - pos == 1) {
          Toffs to = termsOffsets.get(termsOffsets.size() - 1);
          to.setEndOffset(ti.getEndOffset());
        } else {
          termsOffsets.add(new Toffs(ti.getStartOffset(), ti.getEndOffset()));
        }
        pos = ti.getPosition();
      }
    }

    /** Merging constructor. Note that this just grabs seqnum from the first info. */
    public WeightedPhraseInfo(Collection<WeightedPhraseInfo> toMerge) {
      // Pretty much the same idea as merging FieldPhraseLists:
      // Step 1.  Sort by startOffset, endOffset
      //          While we are here merge the boosts and termInfos
      Iterator<WeightedPhraseInfo> toMergeItr = toMerge.iterator();
      if (!toMergeItr.hasNext()) {
        throw new IllegalArgumentException("toMerge must contain at least one WeightedPhraseInfo.");
      }
      WeightedPhraseInfo first = toMergeItr.next();
      @SuppressWarnings({"rawtypes", "unchecked"})
      Iterator<Toffs>[] allToffs = new Iterator[toMerge.size()];
      termsInfos = new ArrayList<>();
      seqnum = first.seqnum;
      boost = first.boost;
      allToffs[0] = first.termsOffsets.iterator();
      int index = 1;
      while (toMergeItr.hasNext()) {
        WeightedPhraseInfo info = toMergeItr.next();
        boost += info.boost;
        termsInfos.addAll(info.termsInfos);
        allToffs[index++] = info.termsOffsets.iterator();
      }
      // Step 2.  Walk the sorted list merging overlaps
      MergedIterator<Toffs> itr = new MergedIterator<>(false, allToffs);
      termsOffsets = new ArrayList<>();
      if (!itr.hasNext()) {
        return;
      }
      Toffs work = itr.next();
      while (itr.hasNext()) {
        Toffs current = itr.next();
        if (current.startOffset <= work.endOffset) {
          work.endOffset = Math.max(work.endOffset, current.endOffset);
        } else {
          termsOffsets.add(work);
          work = current;
        }
      }
      termsOffsets.add(work);
    }

    public int getStartOffset() {
      return termsOffsets.get(0).startOffset;
    }

    public int getEndOffset() {
      return termsOffsets.get(termsOffsets.size() - 1).endOffset;
    }

    public boolean isOffsetOverlap(WeightedPhraseInfo other) {
      int so = getStartOffset();
      int eo = getEndOffset();
      int oso = other.getStartOffset();
      int oeo = other.getEndOffset();
      if (so <= oso && oso < eo) return true;
      if (so < oeo && oeo <= eo) return true;
      if (oso <= so && so < oeo) return true;
      if (oso < eo && eo <= oeo) return true;
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(getText()).append('(').append(boost).append(")(");
      for (Toffs to : termsOffsets) {
        sb.append(to);
      }
      sb.append(')');
      return sb.toString();
    }

    /** @return the seqnum */
    public int getSeqnum() {
      return seqnum;
    }

    @Override
    public int compareTo(WeightedPhraseInfo other) {
      int diff = getStartOffset() - other.getStartOffset();
      if (diff != 0) {
        return diff;
      }
      diff = getEndOffset() - other.getEndOffset();
      if (diff != 0) {
        return diff;
      }
      return (int) Math.signum(getBoost() - other.getBoost());
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getStartOffset();
      result = prime * result + getEndOffset();
      long b = Double.doubleToLongBits(getBoost());
      result = prime * result + (int) (b ^ (b >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      WeightedPhraseInfo other = (WeightedPhraseInfo) obj;
      if (getStartOffset() != other.getStartOffset()) {
        return false;
      }
      if (getEndOffset() != other.getEndOffset()) {
        return false;
      }
      if (getBoost() != other.getBoost()) {
        return false;
      }
      return true;
    }

    /** Term offsets (start + end) */
    public static class Toffs implements Comparable<Toffs> {
      private int startOffset;
      private int endOffset;

      public Toffs(int startOffset, int endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
      }

      public void setEndOffset(int endOffset) {
        this.endOffset = endOffset;
      }

      public int getStartOffset() {
        return startOffset;
      }

      public int getEndOffset() {
        return endOffset;
      }

      @Override
      public int compareTo(Toffs other) {
        int diff = getStartOffset() - other.getStartOffset();
        if (diff != 0) {
          return diff;
        }
        return getEndOffset() - other.getEndOffset();
      }

      @Override
      public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + getStartOffset();
        result = prime * result + getEndOffset();
        return result;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        }
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        Toffs other = (Toffs) obj;
        if (getStartOffset() != other.getStartOffset()) {
          return false;
        }
        if (getEndOffset() != other.getEndOffset()) {
          return false;
        }
        return true;
      }

      @Override
      public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('(').append(startOffset).append(',').append(endOffset).append(')');
        return sb.toString();
      }
    }
  }
}
