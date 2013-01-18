package org.apache.lucene.search.grouping.term;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocTermOrds;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.grouping.AbstractGroupFacetCollector;
import org.apache.lucene.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link AbstractGroupFacetCollector} that computes grouped facets based on the indexed terms
 * from the {@link FieldCache}.
 *
 * @lucene.experimental
 */
public abstract class TermGroupFacetCollector extends AbstractGroupFacetCollector {

  final List<GroupedFacetHit> groupedFacetHits;
  final SentinelIntSet segmentGroupedFacetHits;
  final BytesRef spare = new BytesRef();

  FieldCache.DocTermsIndex groupFieldTermsIndex;

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
    groupedFacetHits = new ArrayList<GroupedFacetHit>(initialSize);
    segmentGroupedFacetHits = new SentinelIntSet(initialSize, -1);
  }

  // Implementation for single valued facet fields.
  static class SV extends TermGroupFacetCollector {

    private FieldCache.DocTermsIndex facetFieldTermsIndex;

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
      int segmentGroupedFacetsIndex = (groupOrd * facetFieldTermsIndex.numOrd()) + facetOrd;
      if (segmentGroupedFacetHits.exists(segmentGroupedFacetsIndex)) {
        return;
      }

      segmentTotalCount++;
      segmentFacetCounts[facetOrd]++;

      segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);
      groupedFacetHits.add(
          new GroupedFacetHit(
              groupOrd == 0 ? null : groupFieldTermsIndex.lookup(groupOrd, new BytesRef()),
              facetOrd == 0 ? null : facetFieldTermsIndex.lookup(facetOrd, new BytesRef())
          )
      );
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      if (segmentFacetCounts != null) {
        segmentResults.add(createSegmentResult());
      }

      groupFieldTermsIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), groupField);
      facetFieldTermsIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), facetField);
      segmentFacetCounts = new int[facetFieldTermsIndex.numOrd()];
      segmentTotalCount = 0;

      segmentGroupedFacetHits.clear();
      for (GroupedFacetHit groupedFacetHit : groupedFacetHits) {
        int facetOrd = facetFieldTermsIndex.binarySearchLookup(groupedFacetHit.facetValue, spare);
        if (facetOrd < 0) {
          continue;
        }

        int groupOrd = groupFieldTermsIndex.binarySearchLookup(groupedFacetHit.groupValue, spare);
        if (groupOrd < 0) {
          continue;
        }

        int segmentGroupedFacetsIndex = (groupOrd * facetFieldTermsIndex.numOrd()) + facetOrd;
        segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);
      }

      if (facetPrefix != null) {
        startFacetOrd = facetFieldTermsIndex.binarySearchLookup(facetPrefix, spare);
        if (startFacetOrd < 0) {
          // Points to the ord one higher than facetPrefix
          startFacetOrd = -startFacetOrd - 1;
        }
        BytesRef facetEndPrefix = BytesRef.deepCopyOf(facetPrefix);
        facetEndPrefix.append(UnicodeUtil.BIG_TERM);
        endFacetOrd = facetFieldTermsIndex.binarySearchLookup(facetEndPrefix, spare);
        endFacetOrd = -endFacetOrd - 1; // Points to the ord one higher than facetEndPrefix
      } else {
        startFacetOrd = 0;
        endFacetOrd = facetFieldTermsIndex.numOrd();
      }
    }

    @Override
    protected SegmentResult createSegmentResult() throws IOException {
      return new SegmentResult(segmentFacetCounts, segmentTotalCount, facetFieldTermsIndex.getTermsEnum(), startFacetOrd, endFacetOrd);
    }

    private static class SegmentResult extends AbstractGroupFacetCollector.SegmentResult {

      final TermsEnum tenum;

      SegmentResult(int[] counts, int total, TermsEnum tenum, int startFacetOrd, int endFacetOrd) throws IOException {
        super(counts, total - counts[0], counts[0], endFacetOrd);
        this.tenum = tenum;
        this.mergePos = startFacetOrd == 0 ? 1 : startFacetOrd;
        if (mergePos < maxTermPos) {
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

  // Implementation for multi valued facet fields.
  static class MV extends TermGroupFacetCollector {

    private DocTermOrds facetFieldDocTermOrds;
    private TermsEnum facetOrdTermsEnum;
    private DocTermOrds.TermOrdsIterator reuse;

    MV(String groupField, String facetField, BytesRef facetPrefix, int initialSize) {
      super(groupField, facetField, facetPrefix, initialSize);
    }

    @Override
    public void collect(int doc) throws IOException {
      int groupOrd = groupFieldTermsIndex.getOrd(doc);
      if (facetFieldDocTermOrds.isEmpty()) {
        int segmentGroupedFacetsIndex = groupOrd * (facetFieldDocTermOrds.numTerms() + 1);
        if (facetPrefix != null || segmentGroupedFacetHits.exists(segmentGroupedFacetsIndex)) {
          return;
        }

        segmentTotalCount++;
        segmentFacetCounts[facetFieldDocTermOrds.numTerms()]++;

        segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);
        groupedFacetHits.add(
            new GroupedFacetHit(groupOrd == 0 ? null : groupFieldTermsIndex.lookup(groupOrd, new BytesRef()), null)
        );
        return;
      }

      if (facetOrdTermsEnum != null) {
        reuse = facetFieldDocTermOrds.lookup(doc, reuse);
      }
      int chunk;
      boolean first = true;
      int[] buffer = new int[5];
      do {
        chunk = reuse != null ? reuse.read(buffer) : 0;
        if (first && chunk == 0) {
          chunk = 1;
          buffer[0] = facetFieldDocTermOrds.numTerms(); // this facet ord is reserved for docs not containing facet field.
        }
        first = false;

        for (int pos = 0; pos < chunk; pos++) {
          int facetOrd = buffer[pos];
          if (facetOrd < startFacetOrd || facetOrd >= endFacetOrd) {
            continue;
          }

          int segmentGroupedFacetsIndex = (groupOrd * (facetFieldDocTermOrds.numTerms() + 1)) + facetOrd;
          if (segmentGroupedFacetHits.exists(segmentGroupedFacetsIndex)) {
            continue;
          }

          segmentTotalCount++;
          segmentFacetCounts[facetOrd]++;

          segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);
          groupedFacetHits.add(
              new GroupedFacetHit(
                  groupOrd == 0 ? null : groupFieldTermsIndex.lookup(groupOrd, new BytesRef()),
                  facetOrd == facetFieldDocTermOrds.numTerms() ? null : BytesRef.deepCopyOf(facetFieldDocTermOrds.lookupTerm(facetOrdTermsEnum, facetOrd))
              )
          );
        }
      } while (chunk >= buffer.length);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      if (segmentFacetCounts != null) {
        segmentResults.add(createSegmentResult());
      }

      reuse = null;
      groupFieldTermsIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), groupField);
      facetFieldDocTermOrds = FieldCache.DEFAULT.getDocTermOrds(context.reader(), facetField);
      facetOrdTermsEnum = facetFieldDocTermOrds.getOrdTermsEnum(context.reader());
      // [facetFieldDocTermOrds.numTerms() + 1] for all possible facet values and docs not containing facet field
      segmentFacetCounts = new int[facetFieldDocTermOrds.numTerms() + 1];
      segmentTotalCount = 0;

      segmentGroupedFacetHits.clear();
      for (GroupedFacetHit groupedFacetHit : groupedFacetHits) {
        int groupOrd = groupFieldTermsIndex.binarySearchLookup(groupedFacetHit.groupValue, spare);
        if (groupOrd < 0) {
          continue;
        }

        int facetOrd;
        if (groupedFacetHit.facetValue != null) {
          if (facetOrdTermsEnum == null || !facetOrdTermsEnum.seekExact(groupedFacetHit.facetValue, true)) {
            continue;
          }
          facetOrd = (int) facetOrdTermsEnum.ord();
        } else {
          facetOrd = facetFieldDocTermOrds.numTerms();
        }

        // (facetFieldDocTermOrds.numTerms() + 1) for all possible facet values and docs not containing facet field
        int segmentGroupedFacetsIndex = (groupOrd * (facetFieldDocTermOrds.numTerms() + 1)) + facetOrd;
        segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);
      }

      if (facetPrefix != null) {
        TermsEnum.SeekStatus seekStatus;
        if (facetOrdTermsEnum != null) {
          seekStatus = facetOrdTermsEnum.seekCeil(facetPrefix, true);
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

        BytesRef facetEndPrefix = BytesRef.deepCopyOf(facetPrefix);
        facetEndPrefix.append(UnicodeUtil.BIG_TERM);
        seekStatus = facetOrdTermsEnum.seekCeil(facetEndPrefix, true);
        if (seekStatus != TermsEnum.SeekStatus.END) {
          endFacetOrd = (int) facetOrdTermsEnum.ord();
        } else {
          endFacetOrd = facetFieldDocTermOrds.numTerms(); // Don't include null...
        }
      } else {
        startFacetOrd = 0;
        endFacetOrd = facetFieldDocTermOrds.numTerms() + 1;
      }
    }

    @Override
    protected SegmentResult createSegmentResult() throws IOException {
      return new SegmentResult(segmentFacetCounts, segmentTotalCount, facetFieldDocTermOrds.numTerms(), facetOrdTermsEnum, startFacetOrd, endFacetOrd);
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