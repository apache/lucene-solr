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
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.KnnGraphWriter;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;

/**
 * The SegmentMerger class combines two or more Segments, represented by an
 * IndexReader, into a single Segment.  Call the merge method to combine the
 * segments.
 *
 * @see #merge
 */
final class SegmentMerger {
  private final Directory directory;

  private final Codec codec;
  
  private final IOContext context;
  
  final MergeState mergeState;
  private final FieldInfos.Builder fieldInfosBuilder;

  // note, just like in codec apis Directory 'dir' is NOT the same as segmentInfo.dir!!
  SegmentMerger(List<CodecReader> readers, SegmentInfo segmentInfo, InfoStream infoStream, Directory dir,
                FieldInfos.FieldNumbers fieldNumbers, IOContext context) throws IOException {
    if (context.context != IOContext.Context.MERGE) {
      throw new IllegalArgumentException("IOContext.context should be MERGE; got: " + context.context);
    }
    mergeState = new MergeState(readers, segmentInfo, infoStream);
    directory = dir;
    this.codec = segmentInfo.getCodec();
    this.context = context;
    this.fieldInfosBuilder = new FieldInfos.Builder(fieldNumbers);
    Version minVersion = Version.LATEST;
    for (CodecReader reader : readers) {
      Version leafMinVersion = reader.getMetaData().getMinVersion();
      if (leafMinVersion == null) {
        minVersion = null;
        break;
      }
      if (minVersion.onOrAfter(leafMinVersion)) {
        minVersion = leafMinVersion;
      }

    }
    assert segmentInfo.minVersion == null : "The min version should be set by SegmentMerger for merged segments";
    segmentInfo.minVersion = minVersion;
    if (mergeState.infoStream.isEnabled("SM")) {
      if (segmentInfo.getIndexSort() != null) {
        mergeState.infoStream.message("SM", "index sort during merge: " + segmentInfo.getIndexSort());
      }
    }
  }
  
  /** True if any merging should happen */
  boolean shouldMerge() {
    return mergeState.segmentInfo.maxDoc() > 0;
  }

  /**
   * Merges the readers into the directory passed to the constructor
   * @return The number of documents that were merged
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  MergeState merge() throws IOException {
    if (!shouldMerge()) {
      throw new IllegalStateException("Merge would result in 0 document segment");
    }
    mergeFieldInfos();

    int numMerged = mergeWithLogging(this::mergeFields, "stored fields");
    assert numMerged == mergeState.segmentInfo.maxDoc(): "numMerged=" + numMerged + " vs mergeState.segmentInfo.maxDoc()=" + mergeState.segmentInfo.maxDoc();

    final SegmentWriteState segmentWriteState = new SegmentWriteState(mergeState.infoStream, directory, mergeState.segmentInfo,
                                                                      mergeState.mergeFieldInfos, null, context);
    final SegmentReadState segmentReadState = new SegmentReadState(directory, mergeState.segmentInfo, mergeState.mergeFieldInfos,
        true, IOContext.READ, segmentWriteState.segmentSuffix, Collections.emptyMap());

    if (mergeState.mergeFieldInfos.hasNorms()) {
      mergeWithLogging(() -> mergeNorms(segmentWriteState), "norms", numMerged);
    }

    mergeWithLogging(() -> mergeTerms(segmentWriteState, segmentReadState), "postings", numMerged);

    if (mergeState.mergeFieldInfos.hasDocValues()) {
      mergeWithLogging(() -> mergeDocValues(segmentWriteState), "doc values", numMerged);
    }

    if (mergeState.mergeFieldInfos.hasPointValues()) {
      mergeWithLogging(() -> mergePoints(segmentWriteState), "points", numMerged);
    }

    if (mergeState.mergeFieldInfos.hasVectorValues()) {
        mergeWithLogging(() -> mergeKnnGraphValues(segmentWriteState), "knn graph", numMerged);
    }

    if (mergeState.mergeFieldInfos.hasVectors()) {
      mergeWithLogging(this::mergeVectors, "term vectors");
    }

    // write the merged infos
    mergeWithLogging(() -> codec.fieldInfosFormat().write(directory, mergeState.segmentInfo, "", mergeState.mergeFieldInfos, context), "field infos", numMerged);

    return mergeState;
  }

  private void mergeDocValues(SegmentWriteState segmentWriteState) throws IOException {
    try (DocValuesConsumer consumer = codec.docValuesFormat().fieldsConsumer(segmentWriteState)) {
      consumer.merge(mergeState);
    }
  }

  private void mergePoints(SegmentWriteState segmentWriteState) throws IOException {
    try (PointsWriter writer = codec.pointsFormat().fieldsWriter(segmentWriteState)) {
      writer.merge(mergeState);
    }
  }

  private void mergeNorms(SegmentWriteState segmentWriteState) throws IOException {
    try (NormsConsumer consumer = codec.normsFormat().normsConsumer(segmentWriteState)) {
      consumer.merge(mergeState);
    }
  }

  private void mergeTerms(SegmentWriteState segmentWriteState, SegmentReadState segmentReadState) throws IOException {
    try (NormsProducer norms = mergeState.mergeFieldInfos.hasNorms()
        ? codec.normsFormat().normsProducer(segmentReadState)
        : null) {
      NormsProducer normsMergeInstance = null;
      if (norms != null) {
        // Use the merge instance in order to reuse the same IndexInput for all terms
        normsMergeInstance = norms.getMergeInstance();
      }
      try (FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(segmentWriteState)) {
        consumer.merge(mergeState, normsMergeInstance);
      }
    }
  }

  public void mergeFieldInfos() throws IOException {
    for (FieldInfos readerFieldInfos : mergeState.fieldInfos) {
      for (FieldInfo fi : readerFieldInfos) {
        fieldInfosBuilder.add(fi);
      }
    }
    mergeState.mergeFieldInfos = fieldInfosBuilder.finish();
  }

  /**
   * Merge stored fields from each of the segments into the new one.
   * @return The number of documents in all of the readers
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  private int mergeFields() throws IOException {
    try (StoredFieldsWriter fieldsWriter = codec.storedFieldsFormat().fieldsWriter(directory, mergeState.segmentInfo, context)) {
      return fieldsWriter.merge(mergeState);
    }
  }

  /**
   * Merge the TermVectors from each of the segments into the new one.
   * @throws IOException if there is a low-level IO error
   */
  private int mergeVectors() throws IOException {
    try (TermVectorsWriter termVectorsWriter = codec.termVectorsFormat().vectorsWriter(directory, mergeState.segmentInfo, context)) {
      int numMerged = termVectorsWriter.merge(mergeState);
      assert numMerged == mergeState.segmentInfo.maxDoc();
      return numMerged;
    }
  }

  private void mergeKnnGraphValues(SegmentWriteState segmentWriteState) throws IOException {
    try (KnnGraphWriter writer = codec.knnGraphFormat().fieldsWriter(segmentWriteState)) {
      writer.merge(mergeState);
    }
  }

  private interface Merger {
    int merge() throws IOException;
  }

  private interface VoidMerger {
    void merge() throws IOException;
  }

  private int mergeWithLogging(Merger merger, String formatName) throws IOException {
    long t0 = 0;
    if (mergeState.infoStream.isEnabled("SM")) {
      t0 = System.nanoTime();
    }
    int numMerged = merger.merge();
    if (mergeState.infoStream.isEnabled("SM")) {
      long t1 = System.nanoTime();
      mergeState.infoStream.message("SM", ((t1 - t0) / 1000000) + " msec to merge vector values [" + numMerged + " docs]");
    }
    return numMerged;
  }

  private void mergeWithLogging(VoidMerger merger, String formatName, int numMerged) throws IOException {
    long t0 = 0;
    if (mergeState.infoStream.isEnabled("SM")) {
      t0 = System.nanoTime();
    }
    merger.merge();
    if (mergeState.infoStream.isEnabled("SM")) {
      long t1 = System.nanoTime();
      mergeState.infoStream.message("SM", ((t1 - t0) / 1000000) + " msec to merge vector values [" + numMerged + " docs]");
    }
  }

}
