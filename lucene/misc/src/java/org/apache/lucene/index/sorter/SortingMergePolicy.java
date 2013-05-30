package org.apache.lucene.index.sorter;

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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;

/** A {@link MergePolicy} that reorders documents according to a {@link Sorter}
 *  before merging them. As a consequence, all segments resulting from a merge
 *  will be sorted while segments resulting from a flush will be in the order
 *  in which documents have been added.
 *  <p><b>NOTE</b>: Never use this {@link MergePolicy} if you rely on
 *  {@link IndexWriter#addDocuments(Iterable, org.apache.lucene.analysis.Analyzer)}
 *  to have sequentially-assigned doc IDs, this policy will scatter doc IDs.
 *  <p><b>NOTE</b>: This {@link MergePolicy} should only be used with idempotent
 *  {@link Sorter}s so that the order of segments is predictable. For example,
 *  using {@link SortingMergePolicy} with {@link Sorter#REVERSE_DOCS} (which is
 *  not idempotent) will make the order of documents in a segment depend on the
 *  number of times the segment has been merged.
 *  @lucene.experimental */
public final class SortingMergePolicy extends MergePolicy {

  /**
   * Put in the {@link SegmentInfo#getDiagnostics() diagnostics} to denote that
   * this segment is sorted.
   */
  public static final String SORTER_ID_PROP = "sorter";
  
  class SortingOneMerge extends OneMerge {

    List<AtomicReader> unsortedReaders;
    Sorter.DocMap docMap;
    AtomicReader sortedView;

    SortingOneMerge(List<SegmentInfoPerCommit> segments) {
      super(segments);
    }

    @Override
    public List<AtomicReader> getMergeReaders() throws IOException {
      if (unsortedReaders == null) {
        unsortedReaders = super.getMergeReaders();
        final AtomicReader atomicView;
        if (unsortedReaders.size() == 1) {
          atomicView = unsortedReaders.get(0);
        } else {
          final IndexReader multiReader = new MultiReader(unsortedReaders.toArray(new AtomicReader[unsortedReaders.size()]));
          atomicView = SlowCompositeReaderWrapper.wrap(multiReader);
        }
        docMap = sorter.sort(atomicView);
        sortedView = SortingAtomicReader.wrap(atomicView, docMap);
      }
      // a null doc map means that the readers are already sorted
      return docMap == null ? unsortedReaders : Collections.singletonList(sortedView);
    }
    
    @Override
    public void setInfo(SegmentInfoPerCommit info) {
      Map<String,String> diagnostics = info.info.getDiagnostics();
      diagnostics.put(SORTER_ID_PROP, sorter.getID());
      super.setInfo(info);
    }

    private MonotonicAppendingLongBuffer getDeletes(List<AtomicReader> readers) {
      MonotonicAppendingLongBuffer deletes = new MonotonicAppendingLongBuffer();
      int deleteCount = 0;
      for (AtomicReader reader : readers) {
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
      return deletes;
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
      final MonotonicAppendingLongBuffer deletes = getDeletes(unsortedReaders);
      return new MergePolicy.DocMap() {
        @Override
        public int map(int old) {
          final int oldWithDeletes = old + (int) deletes.get(old);
          final int newWithDeletes = docMap.oldToNew(oldWithDeletes);
          return mergeState.docMaps[0].get(newWithDeletes);
        }
      };
    }

  }

  class SortingMergeSpecification extends MergeSpecification {

    @Override
    public void add(OneMerge merge) {
      super.add(new SortingOneMerge(merge.segments));
    }

    @Override
    public String segString(Directory dir) {
      return "SortingMergeSpec(" + super.segString(dir) + ", sorter=" + sorter + ")";
    }

  }

  /** Returns true if the given reader is sorted by the given sorter. */
  public static boolean isSorted(AtomicReader reader, Sorter sorter) {
    if (reader instanceof SegmentReader) {
      final SegmentReader segReader = (SegmentReader) reader;
      final Map<String, String> diagnostics = segReader.getSegmentInfo().info.getDiagnostics();
      if (diagnostics != null && sorter.getID().equals(diagnostics.get(SORTER_ID_PROP))) {
        return true;
      }
    }
    return false;
  }

  private MergeSpecification sortedMergeSpecification(MergeSpecification specification) {
    if (specification == null) {
      return null;
    }
    MergeSpecification sortingSpec = new SortingMergeSpecification();
    for (OneMerge merge : specification.merges) {
      sortingSpec.add(merge);
    }
    return sortingSpec;
  }

  final MergePolicy in;
  final Sorter sorter;

  /** Create a new {@link MergePolicy} that sorts documents with <code>sorter</code>. */
  public SortingMergePolicy(MergePolicy in, Sorter sorter) {
    this.in = in;
    this.sorter = sorter;
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger,
      SegmentInfos segmentInfos) throws IOException {
    return sortedMergeSpecification(in.findMerges(mergeTrigger, segmentInfos));
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos,
      int maxSegmentCount, Map<SegmentInfoPerCommit,Boolean> segmentsToMerge)
      throws IOException {
    return sortedMergeSpecification(in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge));
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos)
      throws IOException {
    return sortedMergeSpecification(in.findForcedDeletesMerges(segmentInfos));
  }

  @Override
  public MergePolicy clone() {
    return new SortingMergePolicy(in.clone(), sorter);
  }

  @Override
  public void close() {
    in.close();
  }

  @Override
  public boolean useCompoundFile(SegmentInfos segments,
      SegmentInfoPerCommit newSegment) throws IOException {
    return in.useCompoundFile(segments, newSegment);
  }

  @Override
  public void setIndexWriter(IndexWriter writer) {
    in.setIndexWriter(writer);
  }

  @Override
  public String toString() {
    return "SortingMergePolicy(" + in + ", sorter=" + sorter + ")";
  }

}
