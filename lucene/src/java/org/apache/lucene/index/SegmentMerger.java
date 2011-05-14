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
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.index.MergePolicy.MergeAbortedException;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.MergeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MultiBits;
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

  /** norms header placeholder */
  static final byte[] NORMS_HEADER = new byte[]{'N','R','M',-1};

  private Directory directory;
  private String segment;
  private int termIndexInterval = IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL;

  private List<IndexReader> readers = new ArrayList<IndexReader>();
  private final FieldInfos fieldInfos;

  private int mergedDocs;

  private final MergeState.CheckAbort checkAbort;

  /** Maximum number of contiguous documents to bulk-copy
      when merging stored fields */
  private final static int MAX_RAW_MERGE_DOCS = 4192;

  private Codec codec;
  private SegmentWriteState segmentWriteState;

  private PayloadProcessorProvider payloadProcessorProvider;

  SegmentMerger(Directory dir, int termIndexInterval, String name, MergePolicy.OneMerge merge, PayloadProcessorProvider payloadProcessorProvider, FieldInfos fieldInfos) {
    this.payloadProcessorProvider = payloadProcessorProvider;
    directory = dir;
    segment = name;
    this.fieldInfos = fieldInfos;
    if (merge != null) {
      checkAbort = new MergeState.CheckAbort(merge, directory);
    } else {
      checkAbort = new MergeState.CheckAbort(null, null) {
        @Override
        public void work(double units) throws MergeAbortedException {
          // do nothing
        }
      };
    }
    this.termIndexInterval = termIndexInterval;
  }

  public FieldInfos fieldInfos() {
    return fieldInfos;
  }

  /**
   * Add an IndexReader to the collection of readers that are to be merged
   * @param reader
   */
  final void add(IndexReader reader) {
    ReaderUtil.gatherSubReaders(readers, reader);
  }

  /**
   * Merges the readers specified by the {@link #add} method into the directory passed to the constructor
   * @return The number of documents that were merged
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  final int merge() throws CorruptIndexException, IOException {
    // NOTE: it's important to add calls to
    // checkAbort.work(...) if you make any changes to this
    // method that will spend alot of time.  The frequency
    // of this check impacts how long
    // IndexWriter.close(false) takes to actually stop the
    // threads.

    mergedDocs = mergeFields();
    mergeTerms();
    mergeNorms();

    if (fieldInfos.hasVectors())
      mergeVectors();

    return mergedDocs;
  }

  final Collection<String> createCompoundFile(String fileName, final SegmentInfo info)
          throws IOException {

    // Now merge all added files
    Collection<String> files = info.files();
    CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, fileName, checkAbort);
    for (String file : files) {
      cfsWriter.addFile(file);
    }

    // Perform the merge
    cfsWriter.close();

    return files;
  }

  private static void addIndexed(IndexReader reader, FieldInfos fInfos,
      Collection<String> names, boolean storeTermVectors,
      boolean storePositionWithTermVector, boolean storeOffsetWithTermVector,
      boolean storePayloads, boolean omitTFAndPositions)
      throws IOException {
    for (String field : names) {
      fInfos.addOrUpdate(field, true, storeTermVectors,
          storePositionWithTermVector, storeOffsetWithTermVector, !reader
              .hasNorms(field), storePayloads, omitTFAndPositions);
    }
  }

  private SegmentReader[] matchingSegmentReaders;
  private int[] rawDocLengths;
  private int[] rawDocLengths2;
  private int matchedCount;

  public int getMatchedSubReaderCount() {
    return matchedCount;
  }

  private void setMatchingSegmentReaders() {
    // If the i'th reader is a SegmentReader and has
    // identical fieldName -> number mapping, then this
    // array will be non-null at position i:
    int numReaders = readers.size();
    matchingSegmentReaders = new SegmentReader[numReaders];

    // If this reader is a SegmentReader, and all of its
    // field name -> number mappings match the "merged"
    // FieldInfos, then we can do a bulk copy of the
    // stored fields:
    for (int i = 0; i < numReaders; i++) {
      IndexReader reader = readers.get(i);
      if (reader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) reader;
        boolean same = true;
        FieldInfos segmentFieldInfos = segmentReader.fieldInfos();
        for (FieldInfo fi : segmentFieldInfos) {
          same = fieldInfos.fieldName(fi.number).equals(fi.name);
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
  }

  /**
   *
   * @return The number of documents in all of the readers
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  private int mergeFields() throws CorruptIndexException, IOException {
    for (IndexReader reader : readers) {
      if (reader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) reader;
        FieldInfos readerFieldInfos = segmentReader.fieldInfos();
        for (FieldInfo fi : readerFieldInfos) {
          fieldInfos.add(fi);
        }
      } else {
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR_WITH_POSITION_OFFSET), true, true, true, false, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR_WITH_POSITION), true, true, false, false, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR_WITH_OFFSET), true, false, true, false, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR), true, false, false, false, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.OMIT_TERM_FREQ_AND_POSITIONS), false, false, false, false, true);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.STORES_PAYLOADS), false, false, false, true, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.INDEXED), false, false, false, false, false);
        fieldInfos.addOrUpdate(reader.getFieldNames(FieldOption.UNINDEXED), false);
      }
    }
    final SegmentCodecs codecInfo = fieldInfos.buildSegmentCodecs(false);
    fieldInfos.write(directory, segment + "." + IndexFileNames.FIELD_INFOS_EXTENSION);

    int docCount = 0;

    setMatchingSegmentReaders();

    final FieldsWriter fieldsWriter = new FieldsWriter(directory, segment);

    try {
      int idx = 0;
      for (IndexReader reader : readers) {
        final SegmentReader matchingSegmentReader = matchingSegmentReaders[idx++];
        FieldsReader matchingFieldsReader = null;
        if (matchingSegmentReader != null) {
          final FieldsReader fieldsReader = matchingSegmentReader.getFieldsReader();
          if (fieldsReader != null) {
            matchingFieldsReader = fieldsReader;
          }
        }
        if (reader.hasDeletions()) {
          docCount += copyFieldsWithDeletions(fieldsWriter,
                                              reader, matchingFieldsReader);
        } else {
          docCount += copyFieldsNoDeletions(fieldsWriter,
                                            reader, matchingFieldsReader);
        }
      }
    } finally {
      fieldsWriter.close();
    }

    final String fileName = IndexFileNames.segmentFileName(segment, "", IndexFileNames.FIELDS_INDEX_EXTENSION);
    final long fdxFileLength = directory.fileLength(fileName);

    if (4+((long) docCount)*8 != fdxFileLength)
      // This is most likely a bug in Sun JRE 1.6.0_04/_05;
      // we detect that the bug has struck, here, and
      // throw an exception to prevent the corruption from
      // entering the index.  See LUCENE-1282 for
      // details.
      throw new RuntimeException("mergeFields produced an invalid result: docCount is " + docCount + " but fdx file size is " + fdxFileLength + " file=" + fileName + " file exists?=" + directory.fileExists(fileName) + "; now aborting this merge to prevent index corruption");

    segmentWriteState = new SegmentWriteState(null, directory, segment, fieldInfos, docCount, termIndexInterval, codecInfo, null);

    return docCount;
  }

  private int copyFieldsWithDeletions(final FieldsWriter fieldsWriter, final IndexReader reader,
                                      final FieldsReader matchingFieldsReader)
    throws IOException, MergeAbortedException, CorruptIndexException {
    int docCount = 0;
    final int maxDoc = reader.maxDoc();
    final Bits delDocs = reader.getDeletedDocs();
    if (matchingFieldsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      for (int j = 0; j < maxDoc;) {
        if (delDocs.get(j)) {
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
          if (delDocs.get(j)) {
            j++;
            break;
          }
        } while(numDocs < MAX_RAW_MERGE_DOCS);

        IndexInput stream = matchingFieldsReader.rawDocs(rawDocLengths, start, numDocs);
        fieldsWriter.addRawDocuments(stream, rawDocLengths, numDocs);
        docCount += numDocs;
        checkAbort.work(300 * numDocs);
      }
    } else {
      for (int j = 0; j < maxDoc; j++) {
        if (delDocs.get(j)) {
          // skip deleted docs
          continue;
        }
        // NOTE: it's very important to first assign to doc then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        Document doc = reader.document(j);
        fieldsWriter.addDocument(doc, fieldInfos);
        docCount++;
        checkAbort.work(300);
      }
    }
    return docCount;
  }

  private int copyFieldsNoDeletions(final FieldsWriter fieldsWriter, final IndexReader reader,
                                    final FieldsReader matchingFieldsReader)
    throws IOException, MergeAbortedException, CorruptIndexException {
    final int maxDoc = reader.maxDoc();
    int docCount = 0;
    if (matchingFieldsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      while (docCount < maxDoc) {
        int len = Math.min(MAX_RAW_MERGE_DOCS, maxDoc - docCount);
        IndexInput stream = matchingFieldsReader.rawDocs(rawDocLengths, docCount, len);
        fieldsWriter.addRawDocuments(stream, rawDocLengths, len);
        docCount += len;
        checkAbort.work(300 * len);
      }
    } else {
      for (; docCount < maxDoc; docCount++) {
        // NOTE: it's very important to first assign to doc then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        Document doc = reader.document(docCount);
        fieldsWriter.addDocument(doc, fieldInfos);
        checkAbort.work(300);
      }
    }
    return docCount;
  }

  /**
   * Merge the TermVectors from each of the segments into the new one.
   * @throws IOException
   */
  private final void mergeVectors() throws IOException {
    TermVectorsWriter termVectorsWriter =
      new TermVectorsWriter(directory, segment, fieldInfos);

    try {
      int idx = 0;
      for (final IndexReader reader : readers) {
        final SegmentReader matchingSegmentReader = matchingSegmentReaders[idx++];
        TermVectorsReader matchingVectorsReader = null;
        if (matchingSegmentReader != null) {
          TermVectorsReader vectorsReader = matchingSegmentReader.getTermVectorsReader();

          // If the TV* files are an older format then they cannot read raw docs:
          if (vectorsReader != null && vectorsReader.canReadRawDocs()) {
            matchingVectorsReader = vectorsReader;
          }
        }
        if (reader.hasDeletions()) {
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
                                        final IndexReader reader)
    throws IOException, MergeAbortedException {
    final int maxDoc = reader.maxDoc();
    final Bits delDocs = reader.getDeletedDocs();
    if (matchingVectorsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      for (int docNum = 0; docNum < maxDoc;) {
        if (delDocs.get(docNum)) {
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
          if (delDocs.get(docNum)) {
            docNum++;
            break;
          }
        } while(numDocs < MAX_RAW_MERGE_DOCS);

        matchingVectorsReader.rawDocs(rawDocLengths, rawDocLengths2, start, numDocs);
        termVectorsWriter.addRawDocuments(matchingVectorsReader, rawDocLengths, rawDocLengths2, numDocs);
        checkAbort.work(300 * numDocs);
      }
    } else {
      for (int docNum = 0; docNum < maxDoc; docNum++) {
        if (delDocs.get(docNum)) {
          // skip deleted docs
          continue;
        }

        // NOTE: it's very important to first assign to vectors then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        TermFreqVector[] vectors = reader.getTermFreqVectors(docNum);
        termVectorsWriter.addAllDocVectors(vectors);
        checkAbort.work(300);
      }
    }
  }

  private void copyVectorsNoDeletions(final TermVectorsWriter termVectorsWriter,
                                      final TermVectorsReader matchingVectorsReader,
                                      final IndexReader reader)
      throws IOException, MergeAbortedException {
    final int maxDoc = reader.maxDoc();
    if (matchingVectorsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      int docCount = 0;
      while (docCount < maxDoc) {
        int len = Math.min(MAX_RAW_MERGE_DOCS, maxDoc - docCount);
        matchingVectorsReader.rawDocs(rawDocLengths, rawDocLengths2, docCount, len);
        termVectorsWriter.addRawDocuments(matchingVectorsReader, rawDocLengths, rawDocLengths2, len);
        docCount += len;
        checkAbort.work(300 * len);
      }
    } else {
      for (int docNum = 0; docNum < maxDoc; docNum++) {
        // NOTE: it's very important to first assign to vectors then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        TermFreqVector[] vectors = reader.getTermFreqVectors(docNum);
        termVectorsWriter.addAllDocVectors(vectors);
        checkAbort.work(300);
      }
    }
  }

  SegmentCodecs getSegmentCodecs() {
    assert segmentWriteState != null;
    return segmentWriteState.segmentCodecs;
  }

  private final void mergeTerms() throws CorruptIndexException, IOException {

    // Let CodecProvider decide which codec will be used to write
    // the new segment:

    int docBase = 0;

    final List<Fields> fields = new ArrayList<Fields>();
    final List<ReaderUtil.Slice> slices = new ArrayList<ReaderUtil.Slice>();
    final List<Bits> bits = new ArrayList<Bits>();
    final List<Integer> bitsStarts = new ArrayList<Integer>();

    for(IndexReader r : readers) {
      final Fields f = r.fields();
      final int maxDoc = r.maxDoc();
      if (f != null) {
        slices.add(new ReaderUtil.Slice(docBase, maxDoc, fields.size()));
        fields.add(f);
        bits.add(r.getDeletedDocs());
        bitsStarts.add(docBase);
      }
      docBase += maxDoc;
    }

    bitsStarts.add(docBase);

    // we may gather more readers than mergeState.readerCount
    mergeState = new MergeState();
    mergeState.readers = readers;
    mergeState.readerCount = readers.size();
    mergeState.fieldInfos = fieldInfos;
    mergeState.mergedDocCount = mergedDocs;

    // Remap docIDs
    mergeState.delCounts = new int[mergeState.readerCount];
    mergeState.docMaps = new int[mergeState.readerCount][];
    mergeState.docBase = new int[mergeState.readerCount];
    mergeState.hasPayloadProcessorProvider = payloadProcessorProvider != null;
    mergeState.dirPayloadProcessor = new PayloadProcessorProvider.DirPayloadProcessor[mergeState.readerCount];
    mergeState.currentPayloadProcessor = new PayloadProcessorProvider.PayloadProcessor[mergeState.readerCount];
    mergeState.checkAbort = checkAbort;

    docBase = 0;
    int inputDocBase = 0;

    for(int i=0;i<mergeState.readerCount;i++) {

      final IndexReader reader = readers.get(i);

      mergeState.delCounts[i] = reader.numDeletedDocs();
      mergeState.docBase[i] = docBase;
      docBase += reader.numDocs();
      inputDocBase += reader.maxDoc();
      if (mergeState.delCounts[i] != 0) {
        int delCount = 0;
        final Bits delDocs = reader.getDeletedDocs();
        assert delDocs != null;
        final int maxDoc = reader.maxDoc();
        final int[] docMap = mergeState.docMaps[i] = new int[maxDoc];
        int newDocID = 0;
        for(int j=0;j<maxDoc;j++) {
          if (delDocs.get(j)) {
            docMap[j] = -1;
            delCount++;  // only for assert
          } else {
            docMap[j] = newDocID++;
          }
        }
        assert delCount == mergeState.delCounts[i]: "reader delCount=" + mergeState.delCounts[i] + " vs recomputed delCount=" + delCount;
      }

      if (payloadProcessorProvider != null) {
        mergeState.dirPayloadProcessor[i] = payloadProcessorProvider.getDirProcessor(reader.directory());
      }
    }
    codec = segmentWriteState.segmentCodecs.codec();
    final FieldsConsumer consumer = codec.fieldsConsumer(segmentWriteState);

    // NOTE: this is silly, yet, necessary -- we create a
    // MultiBits as our skip docs only to have it broken
    // apart when we step through the docs enums in
    // MultiDocsEnum.
    mergeState.multiDeletedDocs = new MultiBits(bits, bitsStarts);

    try {
      consumer.merge(mergeState,
                     new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
                                     slices.toArray(ReaderUtil.Slice.EMPTY_ARRAY)));
    } finally {
      consumer.close();
    }
  }

  private MergeState mergeState;

  int[][] getDocMaps() {
    return mergeState.docMaps;
  }

  int[] getDelCounts() {
    return mergeState.delCounts;
  }

  public boolean getAnyNonBulkMerges() {
    assert matchedCount <= readers.size();
    return matchedCount != readers.size();
  }

  private void mergeNorms() throws IOException {
    IndexOutput output = null;
    try {
      for (FieldInfo fi : fieldInfos) {
        if (fi.isIndexed && !fi.omitNorms) {
          if (output == null) {
            output = directory.createOutput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.NORMS_EXTENSION));
            output.writeBytes(NORMS_HEADER,NORMS_HEADER.length);
          }
          for (IndexReader reader : readers) {
            final int maxDoc = reader.maxDoc();
            byte normBuffer[] = reader.norms(fi.name);
            if (normBuffer == null) {
              // Can be null if this segment doesn't have
              // any docs with this field
              normBuffer = new byte[maxDoc];
              Arrays.fill(normBuffer, (byte)0);
            }
            if (!reader.hasDeletions()) {
              //optimized case for segments without deleted docs
              output.writeBytes(normBuffer, maxDoc);
            } else {
              // this segment has deleted docs, so we have to
              // check for every doc if it is deleted or not
              final Bits delDocs = reader.getDeletedDocs();
              for (int k = 0; k < maxDoc; k++) {
                if (!delDocs.get(k)) {
                  output.writeByte(normBuffer[k]);
                }
              }
            }
            checkAbort.work(maxDoc);
          }
        }
      }
    } finally {
      if (output != null) {
        output.close();
      }
    }
  }
}
