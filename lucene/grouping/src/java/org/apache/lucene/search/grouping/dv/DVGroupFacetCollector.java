package org.apache.lucene.search.grouping.dv;

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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.search.grouping.AbstractGroupFacetCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * An implementation of {@link AbstractGroupFacetCollector} that computes grouped facets based on docvalues.
 *
 * @lucene.experimental
 */
public abstract class DVGroupFacetCollector extends AbstractGroupFacetCollector {

  final Type groupDvType;
  final boolean groupDiskResident;
  final Type facetFieldDvType;
  final boolean facetDiskResident;

  final List<GroupedFacetHit> groupedFacetHits;
  final SentinelIntSet segmentGroupedFacetHits;

  /**
   * Factory method for creating the right implementation based on the group docvalues type and the facet docvalues
   * type.
   *
   * Currently only the {@link Type#BYTES_VAR_SORTED} and the {@link Type#BYTES_FIXED_SORTED} are
   * the only docvalues type supported for both the group and facet field.
   *
   * @param groupField        The group field
   * @param groupDvType       The docvalues type for the group field
   * @param groupDiskResident Whether the group docvalues should be disk resident
   * @param facetField        The facet field
   * @param facetDvType       The docvalues type for the facet field
   * @param facetDiskResident Whether the facet docvalues should be disk resident
   * @param facetPrefix       The facet prefix a facet entry should start with to be included.
   * @param initialSize       The initial allocation size of the internal int set and group facet list which should roughly
   *                          match the total number of expected unique groups. Be aware that the heap usage is
   *                          4 bytes * initialSize.
   * @return a <code>DVGroupFacetCollector</code> implementation
   */
  public static DVGroupFacetCollector createDvGroupFacetCollector(String groupField,
                                                                  Type groupDvType,
                                                                  boolean groupDiskResident,
                                                                  String facetField,
                                                                  Type facetDvType,
                                                                  boolean facetDiskResident,
                                                                  BytesRef facetPrefix,
                                                                  int initialSize) {
    switch (groupDvType) {
      case VAR_INTS:
      case FIXED_INTS_8:
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
      case FLOAT_32:
      case FLOAT_64:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_FIXED_DEREF:
      case BYTES_VAR_STRAIGHT:
      case BYTES_VAR_DEREF:
        throw new IllegalArgumentException(String.format(Locale.ROOT, "Group valueType %s not supported", groupDvType));
      case BYTES_VAR_SORTED:
      case BYTES_FIXED_SORTED:
        return GroupSortedBR.createGroupSortedFacetCollector(groupField, groupDvType, groupDiskResident, facetField, facetDvType, facetDiskResident, facetPrefix, initialSize);
      default:
        throw new IllegalArgumentException(String.format(Locale.ROOT, "Group valueType %s not supported", groupDvType));
    }
  }

  DVGroupFacetCollector(String groupField, Type groupDvType, boolean groupDiskResident, String facetField, Type facetFieldDvType, boolean facetDiskResident, BytesRef facetPrefix, int initialSize) {
    super(groupField, facetField, facetPrefix);
    this.groupDvType = groupDvType;
    this.groupDiskResident = groupDiskResident;
    this.facetFieldDvType = facetFieldDvType;
    this.facetDiskResident = facetDiskResident;
    groupedFacetHits = new ArrayList<GroupedFacetHit>(initialSize);
    segmentGroupedFacetHits = new SentinelIntSet(initialSize, -1);
  }

  static abstract class GroupSortedBR extends DVGroupFacetCollector {

    final BytesRef facetSpare = new BytesRef();
    final BytesRef groupSpare = new BytesRef();
    DocValues.SortedSource groupFieldSource;

    GroupSortedBR(String groupField, Type groupDvType, boolean groupDiskResident, String facetField, Type facetFieldDvType, boolean facetDiskResident, BytesRef facetPrefix, int initialSize) {
      super(groupField, groupDvType, groupDiskResident, facetField, facetFieldDvType, facetDiskResident, facetPrefix, initialSize);
    }

    static DVGroupFacetCollector createGroupSortedFacetCollector(String groupField,
                                                                 Type groupDvType,
                                                                 boolean groupDiskResident,
                                                                 String facetField,
                                                                 Type facetDvType,
                                                                 boolean facetDiskResident,
                                                                 BytesRef facetPrefix,
                                                                 int initialSize) {
      switch (facetDvType) {
        case VAR_INTS:
        case FIXED_INTS_8:
        case FIXED_INTS_16:
        case FIXED_INTS_32:
        case FIXED_INTS_64:
        case FLOAT_32:
        case FLOAT_64:
        case BYTES_FIXED_STRAIGHT:
        case BYTES_FIXED_DEREF:
        case BYTES_VAR_STRAIGHT:
        case BYTES_VAR_DEREF:
          throw new IllegalArgumentException(String.format(Locale.ROOT, "Facet valueType %s not supported", facetDvType));
        case BYTES_VAR_SORTED:
        case BYTES_FIXED_SORTED:
          return new FacetSortedBR(groupField, groupDvType, groupDiskResident, facetField, facetDvType, facetDiskResident, facetPrefix, initialSize);
        default:
          throw new IllegalArgumentException(String.format(Locale.ROOT, "Facet valueType %s not supported", facetDvType));
      }
    }


    static class FacetSortedBR extends GroupSortedBR {

      private DocValues.SortedSource facetFieldSource;

      FacetSortedBR(String groupField, Type groupDvType, boolean groupDiskResident, String facetField, Type facetDvType, boolean diskResident, BytesRef facetPrefix, int initialSize) {
        super(groupField, groupDvType, groupDiskResident, facetField, facetDvType, diskResident, facetPrefix, initialSize);
      }

      public void collect(int doc) throws IOException {
        int facetOrd = facetFieldSource.ord(doc);
        if (facetOrd < startFacetOrd || facetOrd >= endFacetOrd) {
          return;
        }

        int groupOrd = groupFieldSource.ord(doc);
        int segmentGroupedFacetsIndex = (groupOrd * facetFieldSource.getValueCount()) + facetOrd;
        if (segmentGroupedFacetHits.exists(segmentGroupedFacetsIndex)) {
          return;
        }

        segmentTotalCount++;
        segmentFacetCounts[facetOrd]++;

        segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);
        groupedFacetHits.add(
            new GroupedFacetHit(
                groupFieldSource.getByOrd(groupOrd, new BytesRef()),
                facetFieldSource.getByOrd(facetOrd, new BytesRef())
            )
        );
      }

      public void setNextReader(AtomicReaderContext context) throws IOException {
        if (segmentFacetCounts != null) {
          segmentResults.add(createSegmentResult());
        }

        groupFieldSource = getDocValuesSortedSource(groupField, groupDvType, groupDiskResident, context.reader());
        facetFieldSource = getDocValuesSortedSource(facetField, facetFieldDvType, facetDiskResident, context.reader());
        segmentFacetCounts = new int[facetFieldSource.getValueCount()];
        segmentTotalCount = 0;

        segmentGroupedFacetHits.clear();
        for (GroupedFacetHit groupedFacetHit : groupedFacetHits) {
          int facetOrd = facetFieldSource.getOrdByValue(groupedFacetHit.facetValue, facetSpare);
          if (facetOrd < 0) {
            continue;
          }

          int groupOrd = groupFieldSource.getOrdByValue(groupedFacetHit.groupValue, groupSpare);
          if (groupOrd < 0) {
            continue;
          }

          int segmentGroupedFacetsIndex = (groupOrd * facetFieldSource.getValueCount()) + facetOrd;
          segmentGroupedFacetHits.put(segmentGroupedFacetsIndex);
        }

        if (facetPrefix != null) {
          startFacetOrd = facetFieldSource.getOrdByValue(facetPrefix, facetSpare);
          if (startFacetOrd < 0) {
            // Points to the ord one higher than facetPrefix
            startFacetOrd = -startFacetOrd - 1;
          }
          BytesRef facetEndPrefix = BytesRef.deepCopyOf(facetPrefix);
          facetEndPrefix.append(UnicodeUtil.BIG_TERM);
          endFacetOrd = facetFieldSource.getOrdByValue(facetEndPrefix, facetSpare);
          endFacetOrd = -endFacetOrd - 1; // Points to the ord one higher than facetEndPrefix
        } else {
          startFacetOrd = 0;
          endFacetOrd = facetFieldSource.getValueCount();
        }
      }

      protected SegmentResult createSegmentResult() throws IOException {
        if (startFacetOrd == 0 && facetFieldSource.getByOrd(startFacetOrd, facetSpare).length == 0) {
          int missing = segmentFacetCounts[0];
          int total = segmentTotalCount - segmentFacetCounts[0];
          return new SegmentResult(segmentFacetCounts, total, missing, facetFieldSource, endFacetOrd);
        } else {
          return new SegmentResult(segmentFacetCounts, segmentTotalCount, facetFieldSource, startFacetOrd, endFacetOrd);
        }
      }

      private DocValues.SortedSource getDocValuesSortedSource(String field, Type dvType, boolean diskResident, AtomicReader reader) throws IOException {
        DocValues dv = reader.docValues(field);
        DocValues.Source dvSource;
        if (dv != null) {
          dvSource = diskResident ? dv.getDirectSource() : dv.getSource();
        } else {
          dvSource = DocValues.getDefaultSortedSource(dvType, reader.maxDoc());
        }
        return dvSource.asSortedSource();
      }

      private static class SegmentResult extends AbstractGroupFacetCollector.SegmentResult {

        final DocValues.SortedSource facetFieldSource;
        final BytesRef spare = new BytesRef();

        SegmentResult(int[] counts, int total, int missing, DocValues.SortedSource facetFieldSource, int endFacetOrd) {
          super(counts, total, missing, endFacetOrd);
          this.facetFieldSource = facetFieldSource;
          this.mergePos = 1;
          if (mergePos < maxTermPos) {
            mergeTerm = facetFieldSource.getByOrd(mergePos, spare);
          }
        }

        SegmentResult(int[] counts, int total, DocValues.SortedSource facetFieldSource, int startFacetOrd, int endFacetOrd) {
          super(counts, total, 0, endFacetOrd);
          this.facetFieldSource = facetFieldSource;
          this.mergePos = startFacetOrd;
          if (mergePos < maxTermPos) {
            mergeTerm = facetFieldSource.getByOrd(mergePos, spare);
          }
        }

        /**
         * {@inheritDoc}
         */
        protected void nextTerm() throws IOException {
          mergeTerm = facetFieldSource.getByOrd(mergePos, spare);
        }

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
