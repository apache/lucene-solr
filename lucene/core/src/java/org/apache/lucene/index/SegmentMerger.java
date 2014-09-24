package org.apache.lucene.index;

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
import java.util.List;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

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
  
  private final MergeState mergeState;
  private final FieldInfos.Builder fieldInfosBuilder;

  // note, just like in codec apis Directory 'dir' is NOT the same as segmentInfo.dir!!
  SegmentMerger(List<LeafReader> readers, SegmentInfo segmentInfo, InfoStream infoStream, Directory dir,
                MergeState.CheckAbort checkAbort, FieldInfos.FieldNumbers fieldNumbers, IOContext context, boolean validate) throws IOException {
    // validate incoming readers
    if (validate) {
      for (LeafReader reader : readers) {
        reader.checkIntegrity();
      }
    }
    mergeState = new MergeState(readers, segmentInfo, infoStream, checkAbort);
    directory = dir;
    this.codec = segmentInfo.getCodec();
    this.context = context;
    this.fieldInfosBuilder = new FieldInfos.Builder(fieldNumbers);
    mergeState.segmentInfo.setDocCount(setDocMaps());
  }
  
  /** True if any merging should happen */
  boolean shouldMerge() {
    return mergeState.segmentInfo.getDocCount() > 0;
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
    // NOTE: it's important to add calls to
    // checkAbort.work(...) if you make any changes to this
    // method that will spend alot of time.  The frequency
    // of this check impacts how long
    // IndexWriter.close(false) takes to actually stop the
    // threads.
    mergeFieldInfos();
    long t0 = 0;
    if (mergeState.infoStream.isEnabled("SM")) {
      t0 = System.nanoTime();
    }
    int numMerged = mergeFields();
    if (mergeState.infoStream.isEnabled("SM")) {
      long t1 = System.nanoTime();
      mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge stored fields [" + numMerged + " docs]");
    }
    assert numMerged == mergeState.segmentInfo.getDocCount();

    final SegmentWriteState segmentWriteState = new SegmentWriteState(mergeState.infoStream, directory, mergeState.segmentInfo,
                                                                      mergeState.fieldInfos, null, context);
    if (mergeState.infoStream.isEnabled("SM")) {
      t0 = System.nanoTime();
    }
    mergeTerms(segmentWriteState);
    if (mergeState.infoStream.isEnabled("SM")) {
      long t1 = System.nanoTime();
      mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge postings [" + numMerged + " docs]");
    }

    if (mergeState.infoStream.isEnabled("SM")) {
      t0 = System.nanoTime();
    }
    if (mergeState.fieldInfos.hasDocValues()) {
      mergeDocValues(segmentWriteState);
    }
    if (mergeState.infoStream.isEnabled("SM")) {
      long t1 = System.nanoTime();
      mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge doc values [" + numMerged + " docs]");
    }
    
    if (mergeState.fieldInfos.hasNorms()) {
      if (mergeState.infoStream.isEnabled("SM")) {
        t0 = System.nanoTime();
      }
      mergeNorms(segmentWriteState);
      if (mergeState.infoStream.isEnabled("SM")) {
        long t1 = System.nanoTime();
        mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge norms [" + numMerged + " docs]");
      }
    }

    if (mergeState.fieldInfos.hasVectors()) {
      if (mergeState.infoStream.isEnabled("SM")) {
        t0 = System.nanoTime();
      }
      numMerged = mergeVectors();
      if (mergeState.infoStream.isEnabled("SM")) {
        long t1 = System.nanoTime();
        mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge vectors [" + numMerged + " docs]");
      }
      assert numMerged == mergeState.segmentInfo.getDocCount();
    }
    
    // write the merged infos
    FieldInfosWriter fieldInfosWriter = codec.fieldInfosFormat().getFieldInfosWriter();
    fieldInfosWriter.write(directory, mergeState.segmentInfo.name, "", mergeState.fieldInfos, context);

    return mergeState;
  }

  private void mergeDocValues(SegmentWriteState segmentWriteState) throws IOException {
    DocValuesConsumer consumer = codec.docValuesFormat().fieldsConsumer(segmentWriteState);
    boolean success = false;
    try {
      consumer.merge(mergeState);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);            
      }
    }
  }

  private void mergeNorms(SegmentWriteState segmentWriteState) throws IOException {
    NormsConsumer consumer = codec.normsFormat().normsConsumer(segmentWriteState);
    boolean success = false;
    try {
      consumer.merge(mergeState);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);            
      }
    }
  }
  
  public void mergeFieldInfos() throws IOException {
    for (LeafReader reader : mergeState.readers) {
      FieldInfos readerFieldInfos = reader.getFieldInfos();
      for (FieldInfo fi : readerFieldInfos) {
        fieldInfosBuilder.add(fi);
      }
    }
    mergeState.fieldInfos = fieldInfosBuilder.finish();
  }

  /**
   * Merge stored fields from each of the segments into the new one.
   * @return The number of documents in all of the readers
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  private int mergeFields() throws IOException {
    final StoredFieldsWriter fieldsWriter = codec.storedFieldsFormat().fieldsWriter(directory, mergeState.segmentInfo, context);
    
    boolean success = false;
    int numDocs;
    try {
      numDocs = fieldsWriter.merge(mergeState);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(fieldsWriter);
      } else {
        IOUtils.closeWhileHandlingException(fieldsWriter);
      }
    }
    return numDocs;
  }

  /**
   * Merge the TermVectors from each of the segments into the new one.
   * @throws IOException if there is a low-level IO error
   */
  private int mergeVectors() throws IOException {
    final TermVectorsWriter termVectorsWriter = codec.termVectorsFormat().vectorsWriter(directory, mergeState.segmentInfo, context);
    
    boolean success = false;
    int numDocs;
    try {
      numDocs = termVectorsWriter.merge(mergeState);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(termVectorsWriter);
      } else {
        IOUtils.closeWhileHandlingException(termVectorsWriter);
      }
    }
    return numDocs;
  }

  // NOTE: removes any "all deleted" readers from mergeState.readers
  private int setDocMaps() throws IOException {
    final int numReaders = mergeState.readers.size();

    // Remap docIDs
    mergeState.docMaps = new MergeState.DocMap[numReaders];
    mergeState.docBase = new int[numReaders];

    int docBase = 0;

    int i = 0;
    while(i < mergeState.readers.size()) {

      final LeafReader reader = mergeState.readers.get(i);

      mergeState.docBase[i] = docBase;
      final MergeState.DocMap docMap = MergeState.DocMap.build(reader);
      mergeState.docMaps[i] = docMap;
      docBase += docMap.numDocs();

      i++;
    }

    return docBase;
  }

  private void mergeTerms(SegmentWriteState segmentWriteState) throws IOException {
    FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(segmentWriteState);
    boolean success = false;
    try {
      consumer.merge(mergeState);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);
      }
    }
  }
}
