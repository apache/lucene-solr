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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/** A {@link MergePolicy} that reorders documents according to a {@link Sort}
 *  before merging them. As a consequence, all segments resulting from a merge
 *  will be sorted while segments resulting from a flush will be in the order
 *  in which documents have been added.
 *  <p><b>NOTE</b>: Never use this policy if you rely on
 *  {@link IndexWriter#addDocuments(Iterable) IndexWriter.addDocuments}
 *  to have sequentially-assigned doc IDs, this policy will scatter doc IDs.
 *  <p><b>NOTE</b>: This policy should only be used with idempotent {@code Sort}s 
 *  so that the order of segments is predictable. For example, using 
 *  {@link Sort#INDEXORDER} in reverse (which is not idempotent) will make 
 *  the order of documents in a segment depend on the number of times the segment 
 *  has been merged.
 *  @lucene.experimental */
public final class SortingMergePolicy extends MergePolicyWrapper {

  /**
   * Put in the {@link SegmentInfo#getDiagnostics() diagnostics} to denote that
   * this segment is sorted.
   */
  public static final String SORTER_ID_PROP = "sorter";
  
  class SortingOneMerge extends OneMerge {

    List<CodecReader> unsortedReaders;
    Sorter.DocMap docMap;
    LeafReader sortedView;
    final InfoStream infoStream;

    SortingOneMerge(List<SegmentCommitInfo> segments, InfoStream infoStream) {
      super(segments);
      this.infoStream = infoStream;
    }

    @Override
    public List<CodecReader> getMergeReaders() throws IOException {
      if (unsortedReaders == null) {
        unsortedReaders = super.getMergeReaders();
        if (infoStream.isEnabled("SMP")) {
          infoStream.message("SMP", "sorting " + unsortedReaders);
          for (LeafReader leaf : unsortedReaders) {
            String sortDescription = getSortDescription(leaf);
            if (sortDescription == null) {
              sortDescription = "not sorted";
            }
            infoStream.message("SMP", "seg=" + leaf + " " + sortDescription);
          }
        }
        // wrap readers, to be optimal for merge;
        List<LeafReader> wrapped = new ArrayList<>(unsortedReaders.size());
        for (LeafReader leaf : unsortedReaders) {
          if (leaf instanceof SegmentReader) {
            leaf = new MergeReaderWrapper((SegmentReader)leaf);
          }
          wrapped.add(leaf);
        }
        final LeafReader atomicView;
        if (wrapped.size() == 1) {
          atomicView = wrapped.get(0);
        } else {
          final CompositeReader multiReader = new MultiReader(wrapped.toArray(new LeafReader[wrapped.size()]));
          atomicView = new SlowCompositeReaderWrapper(multiReader, true);
        }
        docMap = sorter.sort(atomicView);
        sortedView = SortingLeafReader.wrap(atomicView, docMap);
      }
      // a null doc map means that the readers are already sorted
      if (docMap == null) {
        if (infoStream.isEnabled("SMP")) {
          infoStream.message("SMP", "readers already sorted, omitting sort");
        }
        return unsortedReaders;
      } else {
        if (infoStream.isEnabled("SMP")) {
          infoStream.message("SMP", "sorting readers by " + sort);
        }
        return Collections.singletonList(SlowCodecReaderWrapper.wrap(sortedView));
      }
    }
    
    @Override
    public void setMergeInfo(SegmentCommitInfo info) {
      Map<String,String> diagnostics = info.info.getDiagnostics();
      diagnostics.put(SORTER_ID_PROP, sorter.getID());
      super.setMergeInfo(info);
    }

    private PackedLongValues getDeletes(List<CodecReader> readers) {
      PackedLongValues.Builder deletes = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
      int deleteCount = 0;
      for (LeafReader reader : readers) {
        final int maxDoc = reader.maxDoc();
        final Bits liveDocs = reader.getLiveDocs();
        for (int i = 0; i < maxDoc; ++i) {
          if (liveDocs != null && !liveDocs.get(i)) {
            ++deleteCount;
          } else {
            deletes.add(deleteCount);
          }
        }
      }
      return deletes.build();
    }

    @Override
    public MergePolicy.DocMap getDocMap(final MergeState mergeState) {
      if (unsortedReaders == null) {
        throw new IllegalStateException();
      }
      if (docMap == null) {
        return super.getDocMap(mergeState);
      }
      assert mergeState.docMaps.length == 1; // we returned a singleton reader
      final PackedLongValues deletes = getDeletes(unsortedReaders);
      return new MergePolicy.DocMap() {
        @Override
        public int map(int old) {
          final int oldWithDeletes = old + (int) deletes.get(old);
          final int newWithDeletes = docMap.oldToNew(oldWithDeletes);
          return mergeState.docMaps[0].get(newWithDeletes);
        }
      };
    }

    @Override
    public String toString() {
      return "SortingMergePolicy.SortingOneMerge(segments=" + segString() + " sort=" + sort + ")";
    }
  }

  class SortingMergeSpecification extends MergeSpecification {
    final InfoStream infoStream;
    
    SortingMergeSpecification(InfoStream infoStream) {
      this.infoStream = infoStream;
    }

    @Override
    public void add(OneMerge merge) {
      super.add(new SortingOneMerge(merge.segments, infoStream));
    }

    @Override
    public String segString(Directory dir) {
      return "SortingMergeSpec(" + super.segString(dir) + ", sorter=" + sorter + ")";
    }

  }

  /** Returns {@code true} if the given {@code reader} is sorted by the
   *  {@code sort} given. Typically the given {@code sort} would be the
   *  {@link SortingMergePolicy#getSort()} order of a {@link SortingMergePolicy}. */
  public static boolean isSorted(LeafReader reader, Sort sort) {
    String description = getSortDescription(reader);
    if (description != null && description.equals(sort.toString())) {
      return true;
    }
    return false;
  }
  
  private static String getSortDescription(LeafReader reader)  {
    if (reader instanceof SegmentReader) {
      final SegmentReader segReader = (SegmentReader) reader;
      final Map<String, String> diagnostics = segReader.getSegmentInfo().info.getDiagnostics();
      if (diagnostics != null) {
        return diagnostics.get(SORTER_ID_PROP);
      }
    } else if (reader instanceof FilterLeafReader) {
      return getSortDescription(FilterLeafReader.unwrap(reader));
    }
    return null;
  }

  private MergeSpecification sortedMergeSpecification(MergeSpecification specification, InfoStream infoStream) {
    if (specification == null) {
      return null;
    }
    MergeSpecification sortingSpec = new SortingMergeSpecification(infoStream);
    for (OneMerge merge : specification.merges) {
      sortingSpec.add(merge);
    }
    return sortingSpec;
  }

  final Sorter sorter;
  final Sort sort;

  /** Create a new {@code MergePolicy} that sorts documents with the given {@code sort}. */
  public SortingMergePolicy(MergePolicy in, Sort sort) {
    super(in);
    this.sorter = new Sorter(sort);
    this.sort = sort;
  }

  /** Return the {@link Sort} order that is used to sort segments when merging. */
  public Sort getSort() {
    return sort;
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger,
      SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
    return sortedMergeSpecification(in.findMerges(mergeTrigger, segmentInfos, writer), writer.infoStream);
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos,
      int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer)
      throws IOException {
    return sortedMergeSpecification(in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer), writer.infoStream);
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer)
      throws IOException {
    return sortedMergeSpecification(in.findForcedDeletesMerges(segmentInfos, writer), writer.infoStream);
  }

  @Override
  public String toString() {
    return "SortingMergePolicy(" + in + ", sorter=" + sorter + ")";
  }
}
