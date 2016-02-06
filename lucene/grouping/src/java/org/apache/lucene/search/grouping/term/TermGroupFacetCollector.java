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
package org.apache.lucene.search.grouping.term;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.grouping.AbstractGroupFacetCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.SentinelIntSet;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link AbstractGroupFacetCollector} that computes grouped facets based on the indexed terms
 * from DocValues.
 *
 * @lucene.experimental
 */
public abstract class TermGroupFacetCollector extends AbstractGroupFacetCollector {

  final List<GroupedFacetHit> groupedFacetHits;
  final SentinelIntSet segmentGroupedFacetHits;

  SortedDocValues groupFieldTermsIndex;

  /**
   * Factory method for creating the right implementation based on the fact whether the facet field contains
   * multiple tokens per documents.
   *
   * @param groupField The group field
   * @param facetField The facet field
   * @param facetFieldMultivalued Whether the facet field has multiple tokens per document
   * @param facetPrefix The facet prefix a facet entry should start with to be included.
   * @param initialSize The initial allocation size of the internal int set and group facet list which should roughly
   *                    match the total number of expected unique groups. Be aware that the heap usage is
   *                    4 bytes * initialSize.
   * @return <code>TermGroupFacetCollector</code> implementation
   */
  public static TermGroupFacetCollector createTermGroupFacetCollector(String groupField,
                                                                      String facetField,
                                                                      boolean facetFieldMultivalued,
                                                                      BytesRef facetPrefix,
                                                                      int initialSize) {
    if (facetFieldMultivalued) {
      return new MV(groupField, facetField, facetPrefix, initialSize);
    } else {
      return new SV(groupField, facetField, facetPrefix, initialSize);
    }
  }

  TermGroupFacetCollector(String groupField, String facetField, BytesRef facetPrefix, int initialSize) {
    super(groupField, facetField, facetPrefix);
    groupedFacetHits = new ArrayList<>(initialSize);
    segmentGroupedFacetHits = new SentinelIntSet(initialSize, Integer.MIN_VALUE);
  }

  // Implementation for single valued facet fields.
  static class SV extends TermGroupFacetCollector {

    private SortedDocValues facetFieldTermsIndex;

    SV(String groupField, String facetField, BytesRef facetPrefix, int initialSize) {
      super(groupField, facetField, facetPrefix, initialSize);
    }

    @Override
    public void collect(int doc) throws IOException {
      int facetOrd = facetFieldTermsIndex.getOrd(doc);
      if (facetOrd < startFacetOrd || facetOrd >= endFacetOrd) {
        return;
      }

      int groupOrd = groupFieldTermsIndex.getOrd(doc);
      int segmentGroupedFacetsIndex = groupOrd * (facetFieldTermsIndex.getValueCount()+1) + facetOrd;
      if (segmentGroupedFacetHits.exists(segmentGroupedFacetsIndex)) {
        return;
      }

      segmentTotalCount++;
      segmentFacetCounts[facetOrd+1]++;

      segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);

      BytesRef groupKey;
      if (groupOrd == -1) {
        groupKey = null;
      } else {
        groupKey = BytesRef.deepCopyOf(groupFieldTermsIndex.lookupOrd(groupOrd));
      }

      BytesRef facetKey;
      if (facetOrd == -1) {
        facetKey = null;
      } else {
        facetKey = BytesRef.deepCopyOf(facetFieldTermsIndex.lookupOrd(facetOrd));
      }

      groupedFacetHits.add(new GroupedFacetHit(groupKey, facetKey));
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      if (segmentFacetCounts != null) {
        segmentResults.add(createSegmentResult());
      }

      groupFieldTermsIndex = DocValues.getSorted(context.reader(), groupField);
      facetFieldTermsIndex = DocValues.getSorted(context.reader(), facetField);

      // 1+ to allow for the -1 "not set":
      segmentFacetCounts = new int[facetFieldTermsIndex.getValueCount()+1];
      segmentTotalCount = 0;

      segmentGroupedFacetHits.clear();
      for (GroupedFacetHit groupedFacetHit : groupedFacetHits) {
        int facetOrd = groupedFacetHit.facetValue == null ? -1 : facetFieldTermsIndex.lookupTerm(groupedFacetHit.facetValue);
        if (groupedFacetHit.facetValue != null && facetOrd < 0) {
          continue;
        }

        int groupOrd = groupedFacetHit.groupValue == null ? -1 : groupFieldTermsIndex.lookupTerm(groupedFacetHit.groupValue);
        if (groupedFacetHit.groupValue != null && groupOrd < 0) {
          continue;
        }

        int segmentGroupedFacetsIndex = groupOrd * (facetFieldTermsIndex.getValueCount()+1) + facetOrd;
        segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);
      }

      if (facetPrefix != null) {
        startFacetOrd = facetFieldTermsIndex.lookupTerm(facetPrefix);
        if (startFacetOrd < 0) {
          // Points to the ord one higher than facetPrefix
          startFacetOrd = -startFacetOrd - 1;
        }
        BytesRefBuilder facetEndPrefix = new BytesRefBuilder();
        facetEndPrefix.append(facetPrefix);
        facetEndPrefix.append(UnicodeUtil.BIG_TERM);
        endFacetOrd = facetFieldTermsIndex.lookupTerm(facetEndPrefix.get());
        assert endFacetOrd < 0;
        endFacetOrd = -endFacetOrd - 1; // Points to the ord one higher than facetEndPrefix
      } else {
        startFacetOrd = -1;
        endFacetOrd = facetFieldTermsIndex.getValueCount();
      }
    }

    @Override
    protected SegmentResult createSegmentResult() throws IOException {
      return new SegmentResult(segmentFacetCounts, segmentTotalCount, facetFieldTermsIndex.termsEnum(), startFacetOrd, endFacetOrd);
    }

    private static class SegmentResult extends AbstractGroupFacetCollector.SegmentResult {

      final TermsEnum tenum;

      SegmentResult(int[] counts, int total, TermsEnum tenum, int startFacetOrd, int endFacetOrd) throws IOException {
        super(counts, total - counts[0], counts[0], endFacetOrd+1);
        this.tenum = tenum;
        this.mergePos = startFacetOrd == -1 ? 1 : startFacetOrd+1;
        if (mergePos < maxTermPos) {
          assert tenum != null;
          tenum.seekExact(startFacetOrd == -1 ? 0 : startFacetOrd);
          mergeTerm = tenum.term();
        }
      }

      @Override
      protected void nextTerm() throws IOException {
        mergeTerm = tenum.next();
      }
    }
  }

  // Implementation for multi valued facet fields.
  static class MV extends TermGroupFacetCollector {

    private SortedSetDocValues facetFieldDocTermOrds;
    private TermsEnum facetOrdTermsEnum;
    private int facetFieldNumTerms;

    MV(String groupField, String facetField, BytesRef facetPrefix, int initialSize) {
      super(groupField, facetField, facetPrefix, initialSize);
    }

    @Override
    public void collect(int doc) throws IOException {
      int groupOrd = groupFieldTermsIndex.getOrd(doc);
      if (facetFieldNumTerms == 0) {
        int segmentGroupedFacetsIndex = groupOrd * (facetFieldNumTerms + 1);
        if (facetPrefix != null || segmentGroupedFacetHits.exists(segmentGroupedFacetsIndex)) {
          return;
        }

        segmentTotalCount++;
        segmentFacetCounts[facetFieldNumTerms]++;

        segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);
        BytesRef groupKey;
        if (groupOrd == -1) {
          groupKey = null;
        } else {
          groupKey = BytesRef.deepCopyOf(groupFieldTermsIndex.lookupOrd(groupOrd));
        }
        groupedFacetHits.add(new GroupedFacetHit(groupKey, null));
        return;
      }

      facetFieldDocTermOrds.setDocument(doc);
      long ord;
      boolean empty = true;
      while ((ord = facetFieldDocTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        process(groupOrd, (int) ord);
        empty = false;
      }
      
      if (empty) {
        process(groupOrd, facetFieldNumTerms); // this facet ord is reserved for docs not containing facet field.
      }
    }
    
    private void process(int groupOrd, int facetOrd) {
      if (facetOrd < startFacetOrd || facetOrd >= endFacetOrd) {
        return;
      }

      int segmentGroupedFacetsIndex = groupOrd * (facetFieldNumTerms + 1) + facetOrd;
      if (segmentGroupedFacetHits.exists(segmentGroupedFacetsIndex)) {
        return;
      }

      segmentTotalCount++;
      segmentFacetCounts[facetOrd]++;

      segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);

      BytesRef groupKey;
      if (groupOrd == -1) {
        groupKey = null;
      } else {
        groupKey = BytesRef.deepCopyOf(groupFieldTermsIndex.lookupOrd(groupOrd));
      }

      final BytesRef facetValue;
      if (facetOrd == facetFieldNumTerms) {
        facetValue = null;
      } else {
        facetValue = BytesRef.deepCopyOf(facetFieldDocTermOrds.lookupOrd(facetOrd));
      }
      groupedFacetHits.add(new GroupedFacetHit(groupKey, facetValue));
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      if (segmentFacetCounts != null) {
        segmentResults.add(createSegmentResult());
      }

      groupFieldTermsIndex = DocValues.getSorted(context.reader(), groupField);
      facetFieldDocTermOrds = DocValues.getSortedSet(context.reader(), facetField);
      facetFieldNumTerms = (int) facetFieldDocTermOrds.getValueCount();
      if (facetFieldNumTerms == 0) {
        facetOrdTermsEnum = null;
      } else {
        facetOrdTermsEnum = facetFieldDocTermOrds.termsEnum();
      }
      // [facetFieldNumTerms() + 1] for all possible facet values and docs not containing facet field
      segmentFacetCounts = new int[facetFieldNumTerms + 1];
      segmentTotalCount = 0;

      segmentGroupedFacetHits.clear();
      for (GroupedFacetHit groupedFacetHit : groupedFacetHits) {
        int groupOrd = groupedFacetHit.groupValue == null ? -1 : groupFieldTermsIndex.lookupTerm(groupedFacetHit.groupValue);
        if (groupedFacetHit.groupValue != null && groupOrd < 0) {
          continue;
        }

        int facetOrd;
        if (groupedFacetHit.facetValue != null) {
          if (facetOrdTermsEnum == null || !facetOrdTermsEnum.seekExact(groupedFacetHit.facetValue)) {
            continue;
          }
          facetOrd = (int) facetOrdTermsEnum.ord();
        } else {
          facetOrd = facetFieldNumTerms;
        }

        // (facetFieldDocTermOrds.numTerms() + 1) for all possible facet values and docs not containing facet field
        int segmentGroupedFacetsIndex = groupOrd * (facetFieldNumTerms + 1) + facetOrd;
        segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);
      }

      if (facetPrefix != null) {
        TermsEnum.SeekStatus seekStatus;
        if (facetOrdTermsEnum != null) {
          seekStatus = facetOrdTermsEnum.seekCeil(facetPrefix);
        } else {
          seekStatus = TermsEnum.SeekStatus.END;
        }

        if (seekStatus != TermsEnum.SeekStatus.END) {
          startFacetOrd = (int) facetOrdTermsEnum.ord();
        } else {
          startFacetOrd = 0;
          endFacetOrd = 0;
          return;
        }

        BytesRefBuilder facetEndPrefix = new BytesRefBuilder();
        facetEndPrefix.append(facetPrefix);
        facetEndPrefix.append(UnicodeUtil.BIG_TERM);
        seekStatus = facetOrdTermsEnum.seekCeil(facetEndPrefix.get());
        if (seekStatus != TermsEnum.SeekStatus.END) {
          endFacetOrd = (int) facetOrdTermsEnum.ord();
        } else {
          endFacetOrd = facetFieldNumTerms; // Don't include null...
        }
      } else {
        startFacetOrd = 0;
        endFacetOrd = facetFieldNumTerms + 1;
      }
    }

    @Override
    protected SegmentResult createSegmentResult() throws IOException {
      return new SegmentResult(segmentFacetCounts, segmentTotalCount, facetFieldNumTerms, facetOrdTermsEnum, startFacetOrd, endFacetOrd);
    }

    private static class SegmentResult extends AbstractGroupFacetCollector.SegmentResult {

      final TermsEnum tenum;

      SegmentResult(int[] counts, int total, int missingCountIndex, TermsEnum tenum, int startFacetOrd, int endFacetOrd) throws IOException {
        super(counts, total - counts[missingCountIndex], counts[missingCountIndex],
            endFacetOrd == missingCountIndex + 1 ?  missingCountIndex : endFacetOrd);
        this.tenum = tenum;
        this.mergePos = startFacetOrd;
        if (tenum != null) {
          tenum.seekExact(mergePos);
          mergeTerm = tenum.term();
        }
      }

      @Override
      protected void nextTerm() throws IOException {
        mergeTerm = tenum.next();
      }
    }
  }
}

class GroupedFacetHit {

  final BytesRef groupValue;
  final BytesRef facetValue;

  GroupedFacetHit(BytesRef groupValue, BytesRef facetValue) {
    this.groupValue = groupValue;
    this.facetValue = facetValue;
  }
}