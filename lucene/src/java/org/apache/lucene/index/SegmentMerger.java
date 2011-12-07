package org.apache.lucene.index;

/**
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldInfosWriter;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.StoredFieldsWriter;
import org.apache.lucene.index.codecs.PerDocConsumer;
import org.apache.lucene.index.codecs.TermVectorsWriter;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ReaderUtil;

/**
 * The SegmentMerger class combines two or more Segments, represented by an IndexReader ({@link #add},
 * into a single Segment.  After adding the appropriate readers, call the merge method to combine the
 * segments.
 *
 * @see #merge
 * @see #add
 */
final class SegmentMerger {
  private final Directory directory;
  private final String segment;
  private final int termIndexInterval;

  private final Codec codec;
  
  private final IOContext context;
  
  private final MergeState mergeState = new MergeState();

  SegmentMerger(InfoStream infoStream, Directory dir, int termIndexInterval, String name, MergeState.CheckAbort checkAbort, PayloadProcessorProvider payloadProcessorProvider, FieldInfos fieldInfos, Codec codec, IOContext context) {
    mergeState.infoStream = infoStream;
    mergeState.readers = new ArrayList<MergeState.IndexReaderAndLiveDocs>();
    mergeState.fieldInfos = fieldInfos;
    mergeState.checkAbort = checkAbort;
    mergeState.payloadProcessorProvider = payloadProcessorProvider;
    directory = dir;
    segment = name;
    this.termIndexInterval = termIndexInterval;
    this.codec = codec;
    this.context = context;
  }

  /**
   * Add an IndexReader to the collection of readers that are to be merged
   * @param reader
   */
  final void add(IndexReader reader) {
    try {
      new ReaderUtil.Gather(reader) {
        @Override
        protected void add(int base, IndexReader r) {
          mergeState.readers.add(new MergeState.IndexReaderAndLiveDocs(r, r.getLiveDocs()));
        }
      }.run();
    } catch (IOException ioe) {
      // won't happen
      throw new RuntimeException(ioe);
    }
  }

  final void add(SegmentReader reader, Bits liveDocs) {
    mergeState.readers.add(new MergeState.IndexReaderAndLiveDocs(reader, liveDocs));
  }

  /**
   * Merges the readers specified by the {@link #add} method into the directory passed to the constructor
   * @return The number of documents that were merged
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  final MergeState merge() throws CorruptIndexException, IOException {
    // NOTE: it's important to add calls to
    // checkAbort.work(...) if you make any changes to this
    // method that will spend alot of time.  The frequency
    // of this check impacts how long
    // IndexWriter.close(false) takes to actually stop the
    // threads.
    
    final int numReaders = mergeState.readers.size();
    // Remap docIDs
    mergeState.docMaps = new int[numReaders][];
    mergeState.docBase = new int[numReaders];
    mergeState.dirPayloadProcessor = new PayloadProcessorProvider.DirPayloadProcessor[numReaders];
    mergeState.currentPayloadProcessor = new PayloadProcessorProvider.PayloadProcessor[numReaders];

    mergeFieldInfos();
    setMatchingSegmentReaders();
    mergeState.mergedDocCount = mergeFields();

    final SegmentWriteState segmentWriteState = new SegmentWriteState(mergeState.infoStream, directory, segment, mergeState.fieldInfos, mergeState.mergedDocCount, termIndexInterval, codec, null, context);
    mergeTerms(segmentWriteState);
    mergePerDoc(segmentWriteState);
    mergeNorms();

    if (mergeState.fieldInfos.hasVectors()) {
      int numMerged = mergeVectors();
      assert numMerged == mergeState.mergedDocCount;
    }
    // write FIS once merge is done. IDV might change types or drops fields
    FieldInfosWriter fieldInfosWriter = codec.fieldInfosFormat().getFieldInfosWriter();
    fieldInfosWriter.write(directory, segment, mergeState.fieldInfos, context);
    return mergeState;
  }

  private static void addIndexed(IndexReader reader, FieldInfos fInfos,
      Collection<String> names, boolean storeTermVectors,
      boolean storePositionWithTermVector, boolean storeOffsetWithTermVector,
      boolean storePayloads, IndexOptions indexOptions)
      throws IOException {
    for (String field : names) {
      fInfos.addOrUpdate(field, true, storeTermVectors,
          storePositionWithTermVector, storeOffsetWithTermVector, !reader
              .hasNorms(field), storePayloads, indexOptions, null);
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
      MergeState.IndexReaderAndLiveDocs reader = mergeState.readers.get(i);
      if (reader.reader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) reader.reader;
        boolean same = true;
        FieldInfos segmentFieldInfos = segmentReader.fieldInfos();
        for (FieldInfo fi : segmentFieldInfos) {
          if (!mergeState.fieldInfos.fieldName(fi.number).equals(fi.name)) {
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

  private void mergeFieldInfos() throws IOException {
    for (MergeState.IndexReaderAndLiveDocs readerAndLiveDocs : mergeState.readers) {
      final IndexReader reader = readerAndLiveDocs.reader;
      if (reader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) reader;
        FieldInfos readerFieldInfos = segmentReader.fieldInfos();
        for (FieldInfo fi : readerFieldInfos) {
          mergeState.fieldInfos.add(fi);
        }
      } else {
        addIndexed(reader, mergeState.fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR_WITH_POSITION_OFFSET), true, true, true, false, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        addIndexed(reader, mergeState.fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR_WITH_POSITION), true, true, false, false, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        addIndexed(reader, mergeState.fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR_WITH_OFFSET), true, false, true, false, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        addIndexed(reader, mergeState.fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR), true, false, false, false, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        addIndexed(reader, mergeState.fieldInfos, reader.getFieldNames(FieldOption.OMIT_POSITIONS), false, false, false, false, IndexOptions.DOCS_AND_FREQS);
        addIndexed(reader, mergeState.fieldInfos, reader.getFieldNames(FieldOption.OMIT_TERM_FREQ_AND_POSITIONS), false, false, false, false, IndexOptions.DOCS_ONLY);
        addIndexed(reader, mergeState.fieldInfos, reader.getFieldNames(FieldOption.STORES_PAYLOADS), false, false, false, true, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        addIndexed(reader, mergeState.fieldInfos, reader.getFieldNames(FieldOption.INDEXED), false, false, false, false, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        mergeState.fieldInfos.addOrUpdate(reader.getFieldNames(FieldOption.UNINDEXED), false);
        Collection<String> dvNames = reader.getFieldNames(FieldOption.DOC_VALUES);
        mergeState.fieldInfos.addOrUpdate(dvNames, false);
        for (String dvName : dvNames) {
          mergeState.fieldInfos.fieldInfo(dvName).setDocValues(reader.docValues(dvName).type());
        }
      }
    }
  }

  /**
   *
   * @return The number of documents in all of the readers
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  private int mergeFields() throws CorruptIndexException, IOException {
    final StoredFieldsWriter fieldsWriter = codec.storedFieldsFormat().fieldsWriter(directory, segment, context);
    
    try {
      return fieldsWriter.merge(mergeState);
    } finally {
      fieldsWriter.close();
    }
  }

  /**
   * Merge the TermVectors from each of the segments into the new one.
   * @throws IOException
   */
  private final int mergeVectors() throws IOException {
    final TermVectorsWriter termVectorsWriter = codec.termVectorsFormat().vectorsWriter(directory, segment, context);
    
    try {
      return termVectorsWriter.merge(mergeState);
    } finally {
      termVectorsWriter.close();
    }
  }

  private final void mergeTerms(SegmentWriteState segmentWriteState) throws CorruptIndexException, IOException {
    int docBase = 0;
    
    final List<Fields> fields = new ArrayList<Fields>();
    final List<ReaderUtil.Slice> slices = new ArrayList<ReaderUtil.Slice>();

    for(MergeState.IndexReaderAndLiveDocs r : mergeState.readers) {
      final Fields f = r.reader.fields();
      final int maxDoc = r.reader.maxDoc();
      if (f != null) {
        slices.add(new ReaderUtil.Slice(docBase, maxDoc, fields.size()));
        fields.add(f);
      }
      docBase += maxDoc;
    }

    final int numReaders = mergeState.readers.size();

    docBase = 0;

    for(int i=0;i<numReaders;i++) {

      final MergeState.IndexReaderAndLiveDocs reader = mergeState.readers.get(i);

      mergeState.docBase[i] = docBase;
      final int maxDoc = reader.reader.maxDoc();
      if (reader.liveDocs != null) {
        int delCount = 0;
        final Bits liveDocs = reader.liveDocs;
        assert liveDocs != null;
        final int[] docMap = mergeState.docMaps[i] = new int[maxDoc];
        int newDocID = 0;
        for(int j=0;j<maxDoc;j++) {
          if (!liveDocs.get(j)) {
            docMap[j] = -1;
            delCount++;
          } else {
            docMap[j] = newDocID++;
          }
        }
        docBase += maxDoc - delCount;
      } else {
        docBase += maxDoc;
      }

      if (mergeState.payloadProcessorProvider != null) {
        mergeState.dirPayloadProcessor[i] = mergeState.payloadProcessorProvider.getDirProcessor(reader.reader.directory());
      }
    }

    final FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(segmentWriteState);
    boolean success = false;
    try {
      consumer.merge(mergeState,
                     new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
                                     slices.toArray(ReaderUtil.Slice.EMPTY_ARRAY)));
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);
      }
    }
  }

  private void mergePerDoc(SegmentWriteState segmentWriteState) throws IOException {
      final PerDocConsumer docsConsumer = codec.docValuesFormat()
          .docsConsumer(new PerDocWriteState(segmentWriteState));
      // TODO: remove this check when 3.x indexes are no longer supported
      // (3.x indexes don't have docvalues)
      if (docsConsumer == null) {
        return;
      }
      boolean success = false;
      try {
        docsConsumer.merge(mergeState);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(docsConsumer);
        } else {
          IOUtils.closeWhileHandlingException(docsConsumer);
        }
      }
  }

  private void mergeNorms() throws IOException {
    IndexOutput output = null;
    boolean success = false;
    try {
      for (FieldInfo fi : mergeState.fieldInfos) {
        if (fi.isIndexed && !fi.omitNorms) {
          if (output == null) {
            output = directory.createOutput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.NORMS_EXTENSION), context);
            output.writeBytes(SegmentNorms.NORMS_HEADER, SegmentNorms.NORMS_HEADER.length);
          }
          for (MergeState.IndexReaderAndLiveDocs reader : mergeState.readers) {
            final int maxDoc = reader.reader.maxDoc();
            byte normBuffer[] = reader.reader.norms(fi.name);
            if (normBuffer == null) {
              // Can be null if this segment doesn't have
              // any docs with this field
              normBuffer = new byte[maxDoc];
              Arrays.fill(normBuffer, (byte)0);
            }
            if (reader.liveDocs == null) {
              //optimized case for segments without deleted docs
              output.writeBytes(normBuffer, maxDoc);
            } else {
              // this segment has deleted docs, so we have to
              // check for every doc if it is deleted or not
              final Bits liveDocs = reader.liveDocs;
              for (int k = 0; k < maxDoc; k++) {
                if (liveDocs.get(k)) {
                  output.writeByte(normBuffer[k]);
                }
              }
            }
            mergeState.checkAbort.work(maxDoc);
          }
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(output);
      } else {
        IOUtils.closeWhileHandlingException(output);
      }
    }
  }
}
