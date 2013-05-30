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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfo.DocValuesType;
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
  private final int termIndexInterval;

  private final Codec codec;
  
  private final IOContext context;
  
  private final MergeState mergeState;
  private final FieldInfos.Builder fieldInfosBuilder;

  // note, just like in codec apis Directory 'dir' is NOT the same as segmentInfo.dir!!
  SegmentMerger(List<AtomicReader> readers, SegmentInfo segmentInfo, InfoStream infoStream, Directory dir, int termIndexInterval,
                MergeState.CheckAbort checkAbort, FieldInfos.FieldNumbers fieldNumbers, IOContext context) {
    mergeState = new MergeState(readers, segmentInfo, infoStream, checkAbort);
    directory = dir;
    this.termIndexInterval = termIndexInterval;
    this.codec = segmentInfo.getCodec();
    this.context = context;
    this.fieldInfosBuilder = new FieldInfos.Builder(fieldNumbers);
  }

  /**
   * Merges the readers into the directory passed to the constructor
   * @return The number of documents that were merged
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  MergeState merge() throws IOException {
    // NOTE: it's important to add calls to
    // checkAbort.work(...) if you make any changes to this
    // method that will spend alot of time.  The frequency
    // of this check impacts how long
    // IndexWriter.close(false) takes to actually stop the
    // threads.
    
    mergeState.segmentInfo.setDocCount(setDocMaps());
    mergeFieldInfos();
    setMatchingSegmentReaders();
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
                                                                      mergeState.fieldInfos, termIndexInterval, null, context);
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
    fieldInfosWriter.write(directory, mergeState.segmentInfo.name, mergeState.fieldInfos, context);

    return mergeState;
  }

  private void mergeDocValues(SegmentWriteState segmentWriteState) throws IOException {
    DocValuesConsumer consumer = codec.docValuesFormat().fieldsConsumer(segmentWriteState);
    boolean success = false;
    try {
      for (FieldInfo field : mergeState.fieldInfos) {
        DocValuesType type = field.getDocValuesType();
        if (type != null) {
          if (type == DocValuesType.NUMERIC) {
            List<NumericDocValues> toMerge = new ArrayList<NumericDocValues>();
            for (AtomicReader reader : mergeState.readers) {
              NumericDocValues values = reader.getNumericDocValues(field.name);
              if (values == null) {
                values = NumericDocValues.EMPTY;
              }
              toMerge.add(values);
            }
            consumer.mergeNumericField(field, mergeState, toMerge);
          } else if (type == DocValuesType.BINARY) {
            List<BinaryDocValues> toMerge = new ArrayList<BinaryDocValues>();
            for (AtomicReader reader : mergeState.readers) {
              BinaryDocValues values = reader.getBinaryDocValues(field.name);
              if (values == null) {
                values = BinaryDocValues.EMPTY;
              }
              toMerge.add(values);
            }
            consumer.mergeBinaryField(field, mergeState, toMerge);
          } else if (type == DocValuesType.SORTED) {
            List<SortedDocValues> toMerge = new ArrayList<SortedDocValues>();
            for (AtomicReader reader : mergeState.readers) {
              SortedDocValues values = reader.getSortedDocValues(field.name);
              if (values == null) {
                values = SortedDocValues.EMPTY;
              }
              toMerge.add(values);
            }
            consumer.mergeSortedField(field, mergeState, toMerge);
          } else if (type == DocValuesType.SORTED_SET) {
            List<SortedSetDocValues> toMerge = new ArrayList<SortedSetDocValues>();
            for (AtomicReader reader : mergeState.readers) {
              SortedSetDocValues values = reader.getSortedSetDocValues(field.name);
              if (values == null) {
                values = SortedSetDocValues.EMPTY;
              }
              toMerge.add(values);
            }
            consumer.mergeSortedSetField(field, mergeState, toMerge);
          } else {
            throw new AssertionError("type=" + type);
          }
        }
      }
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
    DocValuesConsumer consumer = codec.normsFormat().normsConsumer(segmentWriteState);
    boolean success = false;
    try {
      for (FieldInfo field : mergeState.fieldInfos) {
        if (field.hasNorms()) {
          List<NumericDocValues> toMerge = new ArrayList<NumericDocValues>();
          for (AtomicReader reader : mergeState.readers) {
            NumericDocValues norms = reader.getNormValues(field.name);
            if (norms == null) {
              norms = NumericDocValues.EMPTY;
            }
            toMerge.add(norms);
          }
          consumer.mergeNumericField(field, mergeState, toMerge);
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);            
      }
    }
  }

  private void setMatchingSegmentReaders() {
    // If the i'th reader is a SegmentReader and has
    // identical fieldName -> number mapping, then this
    // array will be non-null at position i:
    int numReaders = mergeState.readers.size();
    mergeState.matchingSegmentReaders = new SegmentReader[numReaders];

    // If this reader is a SegmentReader, and all of its
    // field name -> number mappings match the "merged"
    // FieldInfos, then we can do a bulk copy of the
    // stored fields:
    for (int i = 0; i < numReaders; i++) {
      AtomicReader reader = mergeState.readers.get(i);
      // TODO: we may be able to broaden this to
      // non-SegmentReaders, since FieldInfos is now
      // required?  But... this'd also require exposing
      // bulk-copy (TVs and stored fields) API in foreign
      // readers..
      if (reader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) reader;
        boolean same = true;
        FieldInfos segmentFieldInfos = segmentReader.getFieldInfos();
        for (FieldInfo fi : segmentFieldInfos) {
          FieldInfo other = mergeState.fieldInfos.fieldInfo(fi.number);
          if (other == null || !other.name.equals(fi.name)) {
            same = false;
            break;
          }
        }
        if (same) {
          mergeState.matchingSegmentReaders[i] = segmentReader;
          mergeState.matchedCount++;
        }
      }
    }

    if (mergeState.infoStream.isEnabled("SM")) {
      mergeState.infoStream.message("SM", "merge store matchedCount=" + mergeState.matchedCount + " vs " + mergeState.readers.size());
      if (mergeState.matchedCount != mergeState.readers.size()) {
        mergeState.infoStream.message("SM", "" + (mergeState.readers.size() - mergeState.matchedCount) + " non-bulk merges");
      }
    }
  }
  
  public void mergeFieldInfos() throws IOException {
    for (AtomicReader reader : mergeState.readers) {
      FieldInfos readerFieldInfos = reader.getFieldInfos();
      for (FieldInfo fi : readerFieldInfos) {
        fieldInfosBuilder.add(fi);
      }
    }
    mergeState.fieldInfos = fieldInfosBuilder.finish();
  }

  /**
   *
   * @return The number of documents in all of the readers
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  private int mergeFields() throws IOException {
    final StoredFieldsWriter fieldsWriter = codec.storedFieldsFormat().fieldsWriter(directory, mergeState.segmentInfo, context);
    
    try {
      return fieldsWriter.merge(mergeState);
    } finally {
      fieldsWriter.close();
    }
  }

  /**
   * Merge the TermVectors from each of the segments into the new one.
   * @throws IOException if there is a low-level IO error
   */
  private int mergeVectors() throws IOException {
    final TermVectorsWriter termVectorsWriter = codec.termVectorsFormat().vectorsWriter(directory, mergeState.segmentInfo, context);
    
    try {
      return termVectorsWriter.merge(mergeState);
    } finally {
      termVectorsWriter.close();
    }
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

      final AtomicReader reader = mergeState.readers.get(i);

      mergeState.docBase[i] = docBase;
      final MergeState.DocMap docMap = MergeState.DocMap.build(reader);
      mergeState.docMaps[i] = docMap;
      docBase += docMap.numDocs();

      i++;
    }

    return docBase;
  }

  private void mergeTerms(SegmentWriteState segmentWriteState) throws IOException {
    
    final List<Fields> fields = new ArrayList<Fields>();
    final List<ReaderSlice> slices = new ArrayList<ReaderSlice>();

    int docBase = 0;

    for(int readerIndex=0;readerIndex<mergeState.readers.size();readerIndex++) {
      final AtomicReader reader = mergeState.readers.get(readerIndex);
      final Fields f = reader.fields();
      final int maxDoc = reader.maxDoc();
      if (f != null) {
        slices.add(new ReaderSlice(docBase, maxDoc, readerIndex));
        fields.add(f);
      }
      docBase += maxDoc;
    }

    final FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(segmentWriteState);
    boolean success = false;
    try {
      consumer.merge(mergeState,
                     new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
                                     slices.toArray(ReaderSlice.EMPTY_ARRAY)));
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
