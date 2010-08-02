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
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.index.MergePolicy.MergeAbortedException;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.MergeState;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.MultiBits;

/**
 * The SegmentMerger class combines two or more Segments, represented by an IndexReader ({@link #add},
 * into a single Segment.  After adding the appropriate readers, call the merge method to combine the 
 * segments.
 *<P> 
 * If the compoundFile flag is set, then the segments will be merged into a compound file.
 *   
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
  private FieldInfos fieldInfos;
  
  private int mergedDocs;

  private final CheckAbort checkAbort;

  // Whether we should merge doc stores (stored fields and
  // vectors files).  When all segments we are merging
  // already share the same doc store files, we don't need
  // to merge the doc stores.
  private boolean mergeDocStores;

  /** Maximum number of contiguous documents to bulk-copy
      when merging stored fields */
  private final static int MAX_RAW_MERGE_DOCS = 4192;
  
  private final CodecProvider codecs;
  private Codec codec;
  private SegmentWriteState segmentWriteState;

  private PayloadProcessorProvider payloadProcessorProvider;
  
  SegmentMerger(Directory dir, int termIndexInterval, String name, MergePolicy.OneMerge merge, CodecProvider codecs, PayloadProcessorProvider payloadProcessorProvider) {
    this.payloadProcessorProvider = payloadProcessorProvider;
    directory = dir;
    this.codecs = codecs;
    segment = name;
    if (merge != null) {
      checkAbort = new CheckAbort(merge, directory);
    } else {
      checkAbort = new CheckAbort(null, null) {
        @Override
        public void work(double units) throws MergeAbortedException {
          // do nothing
        }
      };
    }
    this.termIndexInterval = termIndexInterval;
  }
  
  boolean hasProx() {
    return fieldInfos.hasProx();
  }

  /**
   * Add an IndexReader to the collection of readers that are to be merged
   * @param reader
   */
  final void add(IndexReader reader) {
    readers.add(reader);
  }

  /**
   * 
   * @param i The index of the reader to return
   * @return The ith reader to be merged
   */
  final IndexReader segmentReader(int i) {
    return readers.get(i);
  }

  /**
   * Merges the readers specified by the {@link #add} method into the directory passed to the constructor
   * @return The number of documents that were merged
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  final int merge() throws CorruptIndexException, IOException {
    return merge(true);
  }

  /**
   * Merges the readers specified by the {@link #add} method
   * into the directory passed to the constructor.
   * @param mergeDocStores if false, we will not merge the
   * stored fields nor vectors files
   * @return The number of documents that were merged
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  final int merge(boolean mergeDocStores) throws CorruptIndexException, IOException {

    this.mergeDocStores = mergeDocStores;
    
    // NOTE: it's important to add calls to
    // checkAbort.work(...) if you make any changes to this
    // method that will spend alot of time.  The frequency
    // of this check impacts how long
    // IndexWriter.close(false) takes to actually stop the
    // threads.

    mergedDocs = mergeFields();
    mergeTerms();
    mergeNorms();

    if (mergeDocStores && fieldInfos.hasVectors())
      mergeVectors();

    return mergedDocs;
  }

  /**
   * close all IndexReaders that have been added.
   * Should not be called before merge().
   * @throws IOException
   */
  final void closeReaders() throws IOException {
    for (final IndexReader reader : readers) {
      reader.close();
    }
  }

  final List<String> createCompoundFile(String fileName, final SegmentInfo info)
          throws IOException {
    CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, fileName, checkAbort);

    Set<String> fileSet = new HashSet<String>();

    // Basic files
    for (String ext : IndexFileNames.COMPOUND_EXTENSIONS_NOT_CODEC) {
      if (mergeDocStores || (!ext.equals(IndexFileNames.FIELDS_EXTENSION) &&
                             !ext.equals(IndexFileNames.FIELDS_INDEX_EXTENSION)))
        fileSet.add(IndexFileNames.segmentFileName(segment, "", ext));
    }

    codec.files(directory, info, fileSet);
    
    // Fieldable norm files
    int numFIs = fieldInfos.size();
    for (int i = 0; i < numFIs; i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed && !fi.omitNorms) {
        fileSet.add(IndexFileNames.segmentFileName(segment, "", IndexFileNames.NORMS_EXTENSION));
        break;
      }
    }

    // Vector files
    if (fieldInfos.hasVectors() && mergeDocStores) {
      for (String ext : IndexFileNames.VECTOR_EXTENSIONS) {
        fileSet.add(IndexFileNames.segmentFileName(segment, "", ext));
      }
    }

    // Now merge all added files
    for (String file : fileSet) {
      cfsWriter.addFile(file);
    }
    
    // Perform the merge
    cfsWriter.close();
   
    return new ArrayList<String>(fileSet);
  }

  private void addIndexed(IndexReader reader, FieldInfos fInfos,
      Collection<String> names, boolean storeTermVectors,
      boolean storePositionWithTermVector, boolean storeOffsetWithTermVector,
      boolean storePayloads, boolean omitTFAndPositions)
      throws IOException {
    for (String field : names) {
      fInfos.add(field, true, storeTermVectors,
          storePositionWithTermVector, storeOffsetWithTermVector, !reader
              .hasNorms(field), storePayloads, omitTFAndPositions);
    }
  }

  private SegmentReader[] matchingSegmentReaders;
  private int[] rawDocLengths;
  private int[] rawDocLengths2;

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
        int numFieldInfos = segmentFieldInfos.size();
        for (int j = 0; same && j < numFieldInfos; j++) {
          same = fieldInfos.fieldName(j).equals(segmentFieldInfos.fieldName(j));
        }
        if (same) {
          matchingSegmentReaders[i] = segmentReader;
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
  private final int mergeFields() throws CorruptIndexException, IOException {

    if (!mergeDocStores) {
      // When we are not merging by doc stores, their field
      // name -> number mapping are the same.  So, we start
      // with the fieldInfos of the last segment in this
      // case, to keep that numbering.
      final SegmentReader sr = (SegmentReader) readers.get(readers.size()-1);
      fieldInfos = (FieldInfos) sr.core.fieldInfos.clone();
    } else {
      fieldInfos = new FieldInfos();		  // merge field names
    }

    for (IndexReader reader : readers) {
      if (reader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) reader;
        FieldInfos readerFieldInfos = segmentReader.fieldInfos();
        int numReaderFieldInfos = readerFieldInfos.size();
        for (int j = 0; j < numReaderFieldInfos; j++) {
          FieldInfo fi = readerFieldInfos.fieldInfo(j);
          fieldInfos.add(fi.name, fi.isIndexed, fi.storeTermVector,
              fi.storePositionWithTermVector, fi.storeOffsetWithTermVector,
              !reader.hasNorms(fi.name), fi.storePayloads,
              fi.omitTermFreqAndPositions);
        }
      } else {
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR_WITH_POSITION_OFFSET), true, true, true, false, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR_WITH_POSITION), true, true, false, false, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR_WITH_OFFSET), true, false, true, false, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.TERMVECTOR), true, false, false, false, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.OMIT_TERM_FREQ_AND_POSITIONS), false, false, false, false, true);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.STORES_PAYLOADS), false, false, false, true, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(FieldOption.INDEXED), false, false, false, false, false);
        fieldInfos.add(reader.getFieldNames(FieldOption.UNINDEXED), false);
      }
    }
    fieldInfos.write(directory, segment + ".fnm");

    int docCount = 0;

    setMatchingSegmentReaders();

    if (mergeDocStores) {
      // merge field values
      final FieldsWriter fieldsWriter = new FieldsWriter(directory, segment, fieldInfos);

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

    } else {
      // If we are skipping the doc stores, that means there
      // are no deletions in any of these segments, so we
      // just sum numDocs() of each segment to get total docCount
      for (final IndexReader reader : readers) {
        docCount += reader.numDocs();
      }
    }

    segmentWriteState = new SegmentWriteState(null, directory, segment, fieldInfos, null, docCount, 0, termIndexInterval, codecs);

    return docCount;
  }

  private int copyFieldsWithDeletions(final FieldsWriter fieldsWriter, final IndexReader reader,
                                      final FieldsReader matchingFieldsReader)
    throws IOException, MergeAbortedException, CorruptIndexException {
    int docCount = 0;
    final int maxDoc = reader.maxDoc();
    if (matchingFieldsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      for (int j = 0; j < maxDoc;) {
        if (reader.isDeleted(j)) {
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
          if (reader.isDeleted(j)) {
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
        if (reader.isDeleted(j)) {
          // skip deleted docs
          continue;
        }
        // NOTE: it's very important to first assign to doc then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        Document doc = reader.document(j);
        fieldsWriter.addDocument(doc);
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
        fieldsWriter.addDocument(doc);
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
          TermVectorsReader vectorsReader = matchingSegmentReader.getTermVectorsReaderOrig();

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
    if (matchingVectorsReader != null) {
      // We can bulk-copy because the fieldInfos are "congruent"
      for (int docNum = 0; docNum < maxDoc;) {
        if (reader.isDeleted(docNum)) {
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
          if (reader.isDeleted(docNum)) {
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
        if (reader.isDeleted(docNum)) {
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

  Codec getCodec() {
    return codec;
  }

  private final void mergeTerms() throws CorruptIndexException, IOException {

    // Let CodecProvider decide which codec will be used to write
    // the new segment:
    codec = codecs.getWriter(segmentWriteState);
    
    int docBase = 0;

    final List<Fields> fields = new ArrayList<Fields>();
    final List<IndexReader> subReaders = new ArrayList<IndexReader>();
    final List<ReaderUtil.Slice> slices = new ArrayList<ReaderUtil.Slice>();
    final List<Bits> bits = new ArrayList<Bits>();
    final List<Integer> bitsStarts = new ArrayList<Integer>();

    final int numReaders = readers.size();
    for(int i=0;i<numReaders;i++) {
      docBase = new ReaderUtil.Gather(readers.get(i)) {
          @Override
          protected void add(int base, IndexReader r) throws IOException {
            final Fields f = r.fields();
            if (f != null) {
              subReaders.add(r);
              fields.add(f);
              slices.add(new ReaderUtil.Slice(base, r.maxDoc(), fields.size()-1));
              bits.add(r.getDeletedDocs());
              bitsStarts.add(base);
            }
          }
        }.run(docBase);
    }

    bitsStarts.add(docBase);

    // we may gather more readers than mergeState.readerCount
    mergeState = new MergeState();
    mergeState.readers = subReaders;
    mergeState.readerCount = subReaders.size();
    mergeState.fieldInfos = fieldInfos;
    mergeState.mergedDocCount = mergedDocs;
    
    // Remap docIDs
    mergeState.delCounts = new int[mergeState.readerCount];
    mergeState.docMaps = new int[mergeState.readerCount][];
    mergeState.docBase = new int[mergeState.readerCount];
    mergeState.hasPayloadProcessorProvider = payloadProcessorProvider != null;
    mergeState.dirPayloadProcessor = new PayloadProcessorProvider.DirPayloadProcessor[mergeState.readerCount];
    mergeState.currentPayloadProcessor = new PayloadProcessorProvider.PayloadProcessor[mergeState.readerCount];

    docBase = 0;
    int inputDocBase = 0;

    final int[] starts = new int[mergeState.readerCount+1];

    for(int i=0;i<mergeState.readerCount;i++) {

      final IndexReader reader = subReaders.get(i);

      starts[i] = inputDocBase;

      mergeState.delCounts[i] = reader.numDeletedDocs();
      mergeState.docBase[i] = docBase;
      docBase += reader.numDocs();
      inputDocBase += reader.maxDoc();
      if (mergeState.delCounts[i] != 0) {
        int delCount = 0;
        Bits deletedDocs = reader.getDeletedDocs();
        final int maxDoc = reader.maxDoc();
        final int[] docMap = mergeState.docMaps[i] = new int[maxDoc];
        int newDocID = 0;
        for(int j=0;j<maxDoc;j++) {
          if (deletedDocs.get(j)) {
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
    starts[mergeState.readerCount] = inputDocBase;

    final FieldsConsumer consumer = codec.fieldsConsumer(segmentWriteState);

    // NOTE: this is silly, yet, necessary -- we create a
    // MultiBits as our skip docs only to have it broken
    // apart when we step through the docs enums in
    // MultidDcsEnum.... this only matters when we are
    // interacting with a non-core IR subclass, because
    // LegacyFieldsEnum.LegacyDocs[AndPositions]Enum checks
    // that the skipDocs matches the delDocs for the reader
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
  
  private void mergeNorms() throws IOException {
    byte[] normBuffer = null;
    IndexOutput output = null;
    try {
      int numFieldInfos = fieldInfos.size();
      for (int i = 0; i < numFieldInfos; i++) {
        FieldInfo fi = fieldInfos.fieldInfo(i);
        if (fi.isIndexed && !fi.omitNorms) {
          if (output == null) { 
            output = directory.createOutput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.NORMS_EXTENSION));
            output.writeBytes(NORMS_HEADER,NORMS_HEADER.length);
          }
          for ( IndexReader reader : readers) {
            int maxDoc = reader.maxDoc();
            if (normBuffer == null || normBuffer.length < maxDoc) {
              // the buffer is too small for the current segment
              normBuffer = new byte[maxDoc];
            }
            reader.norms(fi.name, normBuffer, 0);
            if (!reader.hasDeletions()) {
              //optimized case for segments without deleted docs
              output.writeBytes(normBuffer, maxDoc);
            } else {
              // this segment has deleted docs, so we have to
              // check for every doc if it is deleted or not
              for (int k = 0; k < maxDoc; k++) {
                if (!reader.isDeleted(k)) {
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

  static class CheckAbort {
    private double workCount;
    private MergePolicy.OneMerge merge;
    private Directory dir;
    public CheckAbort(MergePolicy.OneMerge merge, Directory dir) {
      this.merge = merge;
      this.dir = dir;
    }

    /**
     * Records the fact that roughly units amount of work
     * have been done since this method was last called.
     * When adding time-consuming code into SegmentMerger,
     * you should test different values for units to ensure
     * that the time in between calls to merge.checkAborted
     * is up to ~ 1 second.
     */
    public void work(double units) throws MergePolicy.MergeAbortedException {
      workCount += units;
      if (workCount >= 10000.0) {
        merge.checkAborted(dir);
        workCount = 0;
      }
    }
  }
  
}
