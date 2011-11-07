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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.index.MergePolicy.MergeAbortedException;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsReader;
import org.apache.lucene.index.codecs.FieldsWriter;
import org.apache.lucene.index.codecs.PerDocConsumer;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
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

  /** Maximum number of contiguous documents to bulk-copy
      when merging stored fields */
  private final static int MAX_RAW_MERGE_DOCS = 4192;

  private final Codec codec;

  private final PayloadProcessorProvider payloadProcessorProvider;
  
  private final IOContext context;
  
  private final MergeState mergeState = new MergeState();

  SegmentMerger(InfoStream infoStream, Directory dir, int termIndexInterval, String name, MergePolicy.OneMerge merge, PayloadProcessorProvider payloadProcessorProvider, FieldInfos fieldInfos, Codec codec, IOContext context) {
    mergeState.infoStream = infoStream;
    mergeState.readers = new ArrayList<MergeState.IndexReaderAndLiveDocs>();
    mergeState.fieldInfos = fieldInfos;
    this.payloadProcessorProvider = payloadProcessorProvider;
    directory = dir;
    segment = name;
    if (merge != null) {
      mergeState.checkAbort = new MergeState.CheckAbort(merge, directory);
    } else {
      mergeState.checkAbort = new MergeState.CheckAbort(null, null) {
        @Override
        public void work(double units) throws MergeAbortedException {
          // do nothing
        }
      };
    }
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
    mergeState.hasPayloadProcessorProvider = payloadProcessorProvider != null;
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
      mergeVectors(segmentWriteState);
    }
    // write FIS once merge is done. IDV might change types or drops fields
    mergeState.fieldInfos.write(directory, segment + "." + IndexFileNames.FIELD_INFOS_EXTENSION);
    return mergeState;
  }

  /**
   * NOTE: this method creates a compound file for all files returned by
   * info.files(). While, generally, this may include separate norms and
   * deletion files, this SegmentInfo must not reference such files when this
   * method is called, because they are not allowed within a compound file.
   */
  final Collection<String> createCompoundFile(String fileName, final SegmentInfo info, IOContext context)
          throws IOException {

    // Now merge all added files
    Collection<String> files = info.files();
    CompoundFileDirectory cfsDir = new CompoundFileDirectory(directory, fileName, context, true);
    try {
      for (String file : files) {
        assert !IndexFileNames.matchesExtension(file, IndexFileNames.DELETES_EXTENSION) 
                  : ".del file is not allowed in .cfs: " + file;
        assert !IndexFileNames.isSeparateNormsFile(file) 
                  : "separate norms file (.s[0-9]+) is not allowed in .cfs: " + file;
        directory.copy(cfsDir, file, file, context);
        mergeState.checkAbort.work(directory.fileLength(file));
      }
    } finally {
      cfsDir.close();
    }

    return files;
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

  private SegmentReader[] matchingSegmentReaders;
  private int[] rawDocLengths;
  private int[] rawDocLengths2;
  private int matchedCount;

  private void setMatchingSegmentReaders() {
    // If the i'th reader is a SegmentReader and has
    // identical fieldName -> number mapping, then this
    // array will be non-null at position i:
    int numReaders = mergeState.readers.size();
    matchingSegmentReaders = new SegmentReader[numReaders];

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
          same = mergeState.fieldInfos.fieldName(fi.number).equals(fi.name);
        }
        if (same) {
          matchingSegmentReaders[i] = segmentReader;
          matchedCount++;
        }
      }
    }

    // Used for bulk-reading raw bytes for stored fields
    rawDocLengths = new int[MAX_RAW_MERGE_DOCS];
    rawDocLengths2 = new int[MAX_RAW_MERGE_DOCS];
    if (mergeState.infoStream != null) {
      mergeState.infoStream.message("SM", "merge store matchedCount=" + matchedCount + " vs " + mergeState.readers.size());
      if (matchedCount != mergeState.readers.size()) {
        mergeState.infoStream.message("SM", "" + (mergeState.readers.size() - matchedCount) + " non-bulk merges");
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
        mergeState.fieldInfos.addOrUpdate(reader.getFieldNames(FieldOption.DOC_VALUES), false);
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
    int docCount = 0;

    final FieldsWriter fieldsWriter = codec.fieldsFormat().fieldsWriter(directory, segment, context);
    try {
      int idx = 0;
      for (MergeState.IndexReaderAndLiveDocs reader : mergeState.readers) {
        final SegmentReader matchingSegmentReader = matchingSegmentReaders[idx++];
        FieldsReader matchingFieldsReader = null;
        if (matchingSegmentReader != null) {
          final FieldsReader fieldsReader = matchingSegmentReader.getFieldsReader();
          if (fieldsReader != null) {
            matchingFieldsReader = fieldsReader;
          }
        }
        if (reader.liveDocs != null) {
          docCount += copyFieldsWithDeletions(fieldsWriter,
                                              reader, matchingFieldsReader);
        } else {
          docCount += copyFieldsNoDeletions(fieldsWriter,
                                            reader, matchingFieldsReader);
        }
      }
      fieldsWriter.finish(docCount);
    } finally {
      fieldsWriter.close();
    }

    return docCount;
  }

  private int copyFieldsWithDeletions(final FieldsWriter fieldsWriter, final MergeState.IndexReaderAndLiveDocs reader,
                                      final FieldsReader matchingFieldsReader)
    throws IOException, MergeAbortedException, CorruptIndexException {
    int docCount = 0;
    final int maxDoc = reader.reader.maxDoc();
    final Bits liveDocs = reader.liveDocs;
    assert liveDocs != null;
    if (matchingFieldsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      for (int j = 0; j < maxDoc;) {
        if (!liveDocs.get(j)) {
          // skip deleted docs
          ++j;
          continue;
        }
        // We can optimize this case (doing a bulk byte copy) since the field
        // numbers are identical
        int start = j, numDocs = 0;
        do {
          j++;
          numDocs++;
          if (j >= maxDoc) break;
          if (!liveDocs.get(j)) {
            j++;
            break;
          }
        } while(numDocs < MAX_RAW_MERGE_DOCS);

        IndexInput stream = matchingFieldsReader.rawDocs(rawDocLengths, start, numDocs);
        fieldsWriter.addRawDocuments(stream, rawDocLengths, numDocs);
        docCount += numDocs;
        mergeState.checkAbort.work(300 * numDocs);
      }
    } else {
      for (int j = 0; j < maxDoc; j++) {
        if (!liveDocs.get(j)) {
          // skip deleted docs
          continue;
        }
        // TODO: this could be more efficient using
        // FieldVisitor instead of loading/writing entire
        // doc; ie we just have to renumber the field number
        // on the fly?
        // NOTE: it's very important to first assign to doc then pass it to
        // fieldsWriter.addDocument; see LUCENE-1282
        Document doc = reader.reader.document(j);
        fieldsWriter.addDocument(doc, mergeState.fieldInfos);
        docCount++;
        mergeState.checkAbort.work(300);
      }
    }
    return docCount;
  }

  private int copyFieldsNoDeletions(final FieldsWriter fieldsWriter, final MergeState.IndexReaderAndLiveDocs reader,
                                    final FieldsReader matchingFieldsReader)
    throws IOException, MergeAbortedException, CorruptIndexException {
    final int maxDoc = reader.reader.maxDoc();
    int docCount = 0;
    if (matchingFieldsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      while (docCount < maxDoc) {
        int len = Math.min(MAX_RAW_MERGE_DOCS, maxDoc - docCount);
        IndexInput stream = matchingFieldsReader.rawDocs(rawDocLengths, docCount, len);
        fieldsWriter.addRawDocuments(stream, rawDocLengths, len);
        docCount += len;
        mergeState.checkAbort.work(300 * len);
      }
    } else {
      for (; docCount < maxDoc; docCount++) {
        // NOTE: it's very important to first assign to doc then pass it to
        // fieldsWriter.addDocument; see LUCENE-1282
        Document doc = reader.reader.document(docCount);
        fieldsWriter.addDocument(doc, mergeState.fieldInfos);
        mergeState.checkAbort.work(300);
      }
    }
    return docCount;
  }

  /**
   * Merge the TermVectors from each of the segments into the new one.
   * @throws IOException
   */
  private final void mergeVectors(SegmentWriteState segmentWriteState) throws IOException {
    TermVectorsWriter termVectorsWriter = new TermVectorsWriter(directory, segment, mergeState.fieldInfos, context);

    try {
      int idx = 0;
      for (final MergeState.IndexReaderAndLiveDocs reader : mergeState.readers) {
        final SegmentReader matchingSegmentReader = matchingSegmentReaders[idx++];
        TermVectorsReader matchingVectorsReader = null;
        if (matchingSegmentReader != null) {
          TermVectorsReader vectorsReader = matchingSegmentReader.getTermVectorsReader();

          // If the TV* files are an older format then they cannot read raw docs:
          if (vectorsReader != null && vectorsReader.canReadRawDocs()) {
            matchingVectorsReader = vectorsReader;
          }
        }
        if (reader.liveDocs != null) {
          copyVectorsWithDeletions(termVectorsWriter, matchingVectorsReader, reader);
        } else {
          copyVectorsNoDeletions(termVectorsWriter, matchingVectorsReader, reader);
        }
      }
    } finally {
      termVectorsWriter.close();
    }

    final String fileName = IndexFileNames.segmentFileName(segment, "", IndexFileNames.VECTORS_INDEX_EXTENSION);
    final long tvxSize = directory.fileLength(fileName);
    final int mergedDocs = segmentWriteState.numDocs;

    if (4+((long) mergedDocs)*16 != tvxSize)
      // This is most likely a bug in Sun JRE 1.6.0_04/_05;
      // we detect that the bug has struck, here, and
      // throw an exception to prevent the corruption from
      // entering the index.  See LUCENE-1282 for
      // details.
      throw new RuntimeException("mergeVectors produced an invalid result: mergedDocs is " + mergedDocs + " but tvx size is " + tvxSize + " file=" + fileName + " file exists?=" + directory.fileExists(fileName) + "; now aborting this merge to prevent index corruption");
  }

  private void copyVectorsWithDeletions(final TermVectorsWriter termVectorsWriter,
                                        final TermVectorsReader matchingVectorsReader,
                                        final MergeState.IndexReaderAndLiveDocs reader)
    throws IOException, MergeAbortedException {
    final int maxDoc = reader.reader.maxDoc();
    final Bits liveDocs = reader.liveDocs;
    if (matchingVectorsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      for (int docNum = 0; docNum < maxDoc;) {
        if (!liveDocs.get(docNum)) {
          // skip deleted docs
          ++docNum;
          continue;
        }
        // We can optimize this case (doing a bulk byte copy) since the field
        // numbers are identical
        int start = docNum, numDocs = 0;
        do {
          docNum++;
          numDocs++;
          if (docNum >= maxDoc) break;
          if (!liveDocs.get(docNum)) {
            docNum++;
            break;
          }
        } while(numDocs < MAX_RAW_MERGE_DOCS);

        matchingVectorsReader.rawDocs(rawDocLengths, rawDocLengths2, start, numDocs);
        termVectorsWriter.addRawDocuments(matchingVectorsReader, rawDocLengths, rawDocLengths2, numDocs);
        mergeState.checkAbort.work(300 * numDocs);
      }
    } else {
      for (int docNum = 0; docNum < maxDoc; docNum++) {
        if (!liveDocs.get(docNum)) {
          // skip deleted docs
          continue;
        }

        // NOTE: it's very important to first assign to vectors then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        TermFreqVector[] vectors = reader.reader.getTermFreqVectors(docNum);
        termVectorsWriter.addAllDocVectors(vectors);
        mergeState.checkAbort.work(300);
      }
    }
  }

  private void copyVectorsNoDeletions(final TermVectorsWriter termVectorsWriter,
                                      final TermVectorsReader matchingVectorsReader,
                                      final MergeState.IndexReaderAndLiveDocs reader)
      throws IOException, MergeAbortedException {
    final int maxDoc = reader.reader.maxDoc();
    if (matchingVectorsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      int docCount = 0;
      while (docCount < maxDoc) {
        int len = Math.min(MAX_RAW_MERGE_DOCS, maxDoc - docCount);
        matchingVectorsReader.rawDocs(rawDocLengths, rawDocLengths2, docCount, len);
        termVectorsWriter.addRawDocuments(matchingVectorsReader, rawDocLengths, rawDocLengths2, len);
        docCount += len;
        mergeState.checkAbort.work(300 * len);
      }
    } else {
      for (int docNum = 0; docNum < maxDoc; docNum++) {
        // NOTE: it's very important to first assign to vectors then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        TermFreqVector[] vectors = reader.reader.getTermFreqVectors(docNum);
        termVectorsWriter.addAllDocVectors(vectors);
        mergeState.checkAbort.work(300);
      }
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

      if (payloadProcessorProvider != null) {
        mergeState.dirPayloadProcessor[i] = payloadProcessorProvider.getDirProcessor(reader.reader.directory());
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
