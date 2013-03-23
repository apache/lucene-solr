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
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.Directory;

/** A {@link MergePolicy} that reorders documents according to a {@link Sorter}
 *  before merging them. As a consequence, all segments resulting from a merge
 *  will be sorted while segments resulting from a flush will be in the order
 *  in which documents have been added.
 *  <p>Never use this {@link MergePolicy} if you rely on
 *  {@link IndexWriter#addDocuments(Iterable, org.apache.lucene.analysis.Analyzer)}
 *  to have sequentially-assigned doc IDs, this policy will scatter doc IDs.
 *  @lucene.experimental */
public final class SortingMergePolicy extends MergePolicy {

  private class SortingOneMerge extends OneMerge {

    SortingOneMerge(List<SegmentInfoPerCommit> segments) {
      super(segments);
    }

    @Override
    public List<AtomicReader> getMergeReaders() throws IOException {
      final List<AtomicReader> readers =  super.getMergeReaders();
      switch (readers.size()) {
        case 0:
          return readers;
        case 1:
          return Collections.singletonList(SortingAtomicReader.wrap(readers.get(0), sorter));
        default:
          final IndexReader multiReader = new MultiReader(readers.toArray(new AtomicReader[readers.size()]));
          final AtomicReader atomicReader = SlowCompositeReaderWrapper.wrap(multiReader);
          final AtomicReader sortingReader = SortingAtomicReader.wrap(atomicReader, sorter);
          if (sortingReader == atomicReader) {
            // already sorted, return the original list of readers so that
            // codec-specific bulk-merge methods can be used
            return readers;
          }
          return Collections.singletonList(sortingReader);
      }
    }

  }

  private class SortingMergeSpecification extends MergeSpecification {

    @Override
    public void add(OneMerge merge) {
      super.add(new SortingOneMerge(merge.segments));
    }

    @Override
    public String segString(Directory dir) {
      return "SortingMergeSpec(" + super.segString(dir) + ", sorter=" + sorter + ")";
    }

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

  private final MergePolicy in;
  private final Sorter sorter;

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
