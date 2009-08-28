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
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.index.MergePolicy.MergeAbortedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

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
  private int termIndexInterval = IndexWriter.DEFAULT_TERM_INDEX_INTERVAL;

  private List readers = new ArrayList();
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

  /** This ctor used only by test code.
   * 
   * @param dir The Directory to merge the other segments into
   * @param name The name of the new segment
   */
  SegmentMerger(Directory dir, String name) {
    directory = dir;
    segment = name;
    checkAbort = new CheckAbort(null, null) {
      public void work(double units) throws MergeAbortedException {
        // do nothing
      }
    };
  }

  SegmentMerger(IndexWriter writer, String name, MergePolicy.OneMerge merge) {
    directory = writer.getDirectory();
    segment = name;
    if (merge != null) {
      checkAbort = new CheckAbort(merge, directory);
    } else {
      checkAbort = new CheckAbort(null, null) {
        public void work(double units) throws MergeAbortedException {
          // do nothing
        }
      };
    }
    termIndexInterval = writer.getTermIndexInterval();
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
    return (IndexReader) readers.get(i);
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
    for (Iterator iter = readers.iterator(); iter.hasNext();) {
      ((IndexReader) iter.next()).close();
    }
  }

  final List createCompoundFile(String fileName)
          throws IOException {
    CompoundFileWriter cfsWriter =
      new CompoundFileWriter(directory, fileName, checkAbort);

    List files =
      new ArrayList(IndexFileNames.COMPOUND_EXTENSIONS.length + 1);    
    
    // Basic files
    for (int i = 0; i < IndexFileNames.COMPOUND_EXTENSIONS.length; i++) {
      String ext = IndexFileNames.COMPOUND_EXTENSIONS[i];

      if (ext.equals(IndexFileNames.PROX_EXTENSION) && !hasProx())
        continue;

      if (mergeDocStores || (!ext.equals(IndexFileNames.FIELDS_EXTENSION) &&
                            !ext.equals(IndexFileNames.FIELDS_INDEX_EXTENSION)))
        files.add(segment + "." + ext);
    }

    // Fieldable norm files
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed && !fi.omitNorms) {
        files.add(segment + "." + IndexFileNames.NORMS_EXTENSION);
        break;
      }
    }

    // Vector files
    if (fieldInfos.hasVectors() && mergeDocStores) {
      for (int i = 0; i < IndexFileNames.VECTOR_EXTENSIONS.length; i++) {
        files.add(segment + "." + IndexFileNames.VECTOR_EXTENSIONS[i]);
      }
    }

    // Now merge all added files
    Iterator it = files.iterator();
    while (it.hasNext()) {
      cfsWriter.addFile((String) it.next());
    }
    
    // Perform the merge
    cfsWriter.close();
   
    return files;
  }

  private void addIndexed(IndexReader reader, FieldInfos fInfos,
      Collection names, boolean storeTermVectors,
      boolean storePositionWithTermVector, boolean storeOffsetWithTermVector,
      boolean storePayloads, boolean omitTFAndPositions)
      throws IOException {
    Iterator i = names.iterator();
    while (i.hasNext()) {
      String field = (String) i.next();
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
      IndexReader reader = (IndexReader) readers.get(i);
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
      // When we are not merging by doc stores, that means
      // all segments were written as part of a single
      // autoCommit=false IndexWriter session, so their field
      // name -> number mapping are the same.  So, we start
      // with the fieldInfos of the last segment in this
      // case, to keep that numbering.
      final SegmentReader sr = (SegmentReader) readers.get(readers.size()-1);
      fieldInfos = (FieldInfos) sr.core.fieldInfos.clone();
    } else {
      fieldInfos = new FieldInfos();		  // merge field names
    }

    for (Iterator iter = readers.iterator(); iter.hasNext();) {
      IndexReader reader = (IndexReader) iter.next();
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

      // for merging we don't want to compress/uncompress the data, so to tell the FieldsReader that we're
      // in  merge mode, we use this FieldSelector
      FieldSelector fieldSelectorMerge = new FieldSelector() {
          public FieldSelectorResult accept(String fieldName) {
            return FieldSelectorResult.LOAD_FOR_MERGE;
          }        
        };

      // merge field values
      final FieldsWriter fieldsWriter = new FieldsWriter(directory, segment, fieldInfos);

      try {
        int idx = 0;
        for (Iterator iter = readers.iterator(); iter.hasNext();) {
          final IndexReader reader = (IndexReader) iter.next();
          final SegmentReader matchingSegmentReader = matchingSegmentReaders[idx++];
          FieldsReader matchingFieldsReader = null;
          if (matchingSegmentReader != null) {
            final FieldsReader fieldsReader = matchingSegmentReader.getFieldsReader();
            if (fieldsReader != null && fieldsReader.canReadRawDocs()) {            
              matchingFieldsReader = fieldsReader;
            }
          }
          if (reader.hasDeletions()) {
            docCount += copyFieldsWithDeletions(fieldSelectorMerge, fieldsWriter,
                                                reader, matchingFieldsReader);
          } else {
            docCount += copyFieldsNoDeletions(fieldSelectorMerge, fieldsWriter,
                                              reader, matchingFieldsReader);
          }
        }
      } finally {
        fieldsWriter.close();
      }

      final String fileName = segment + "." + IndexFileNames.FIELDS_INDEX_EXTENSION;
      final long fdxFileLength = directory.fileLength(fileName);

      if (4+((long) docCount)*8 != fdxFileLength)
        // This is most likely a bug in Sun JRE 1.6.0_04/_05;
        // we detect that the bug has struck, here, and
        // throw an exception to prevent the corruption from
        // entering the index.  See LUCENE-1282 for
        // details.
        throw new RuntimeException("mergeFields produced an invalid result: docCount is " + docCount + " but fdx file size is " + fdxFileLength + " file=" + fileName + " file exists?=" + directory.fileExists(fileName) + "; now aborting this merge to prevent index corruption");

    } else
      // If we are skipping the doc stores, that means there
      // are no deletions in any of these segments, so we
      // just sum numDocs() of each segment to get total docCount
      for (Iterator iter = readers.iterator(); iter.hasNext();) {
        docCount += ((IndexReader) iter.next()).numDocs();
      }

    return docCount;
  }

  private int copyFieldsWithDeletions(final FieldSelector fieldSelectorMerge,
                                      final FieldsWriter fieldsWriter, final IndexReader reader,
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
        Document doc = reader.document(j, fieldSelectorMerge);
        fieldsWriter.addDocument(doc);
        docCount++;
        checkAbort.work(300);
      }
    }
    return docCount;
  }

  private int copyFieldsNoDeletions(FieldSelector fieldSelectorMerge,
                                    final FieldsWriter fieldsWriter, final IndexReader reader,
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
        Document doc = reader.document(docCount, fieldSelectorMerge);
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
      for (Iterator iter = readers.iterator(); iter.hasNext();) {
        final SegmentReader matchingSegmentReader = matchingSegmentReaders[idx++];
        TermVectorsReader matchingVectorsReader = null;
        if (matchingSegmentReader != null) {
          TermVectorsReader vectorsReader = matchingSegmentReader.getTermVectorsReaderOrig();

          // If the TV* files are an older format then they cannot read raw docs:
          if (vectorsReader != null && vectorsReader.canReadRawDocs()) {
            matchingVectorsReader = vectorsReader;
          }
        }
        final IndexReader reader = (IndexReader) iter.next();
        if (reader.hasDeletions()) {
          copyVectorsWithDeletions(termVectorsWriter, matchingVectorsReader, reader);
        } else {
          copyVectorsNoDeletions(termVectorsWriter, matchingVectorsReader, reader);
          
        }
      }
    } finally {
      termVectorsWriter.close();
    }

    final String fileName = segment + "." + IndexFileNames.VECTORS_INDEX_EXTENSION;
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

  private SegmentMergeQueue queue = null;

  private final void mergeTerms() throws CorruptIndexException, IOException {

    SegmentWriteState state = new SegmentWriteState(null, directory, segment, null, mergedDocs, 0, termIndexInterval);

    final FormatPostingsFieldsConsumer consumer = new FormatPostingsFieldsWriter(state, fieldInfos);

    try {
      queue = new SegmentMergeQueue(readers.size());

      mergeTermInfos(consumer);

    } finally {
      consumer.finish();
      if (queue != null) queue.close();
    }
  }

  boolean omitTermFreqAndPositions;

  private final void mergeTermInfos(final FormatPostingsFieldsConsumer consumer) throws CorruptIndexException, IOException {
    int base = 0;
    final int readerCount = readers.size();
    for (int i = 0; i < readerCount; i++) {
      IndexReader reader = (IndexReader) readers.get(i);
      TermEnum termEnum = reader.terms();
      SegmentMergeInfo smi = new SegmentMergeInfo(base, termEnum, reader);
      int[] docMap  = smi.getDocMap();
      if (docMap != null) {
        if (docMaps == null) {
          docMaps = new int[readerCount][];
          delCounts = new int[readerCount];
        }
        docMaps[i] = docMap;
        delCounts[i] = smi.reader.maxDoc() - smi.reader.numDocs();
      }
      
      base += reader.numDocs();

      assert reader.numDocs() == reader.maxDoc() - smi.delCount;

      if (smi.next())
        queue.add(smi);				  // initialize queue
      else
        smi.close();
    }

    SegmentMergeInfo[] match = new SegmentMergeInfo[readers.size()];

    String currentField = null;
    FormatPostingsTermsConsumer termsConsumer = null;

    while (queue.size() > 0) {
      int matchSize = 0;			  // pop matching terms
      match[matchSize++] = (SegmentMergeInfo) queue.pop();
      Term term = match[0].term;
      SegmentMergeInfo top = (SegmentMergeInfo) queue.top();

      while (top != null && term.compareTo(top.term) == 0) {
        match[matchSize++] = (SegmentMergeInfo) queue.pop();
        top = (SegmentMergeInfo) queue.top();
      }

      if (currentField != term.field) {
        currentField = term.field;
        if (termsConsumer != null)
          termsConsumer.finish();
        final FieldInfo fieldInfo = fieldInfos.fieldInfo(currentField);
        termsConsumer = consumer.addField(fieldInfo);
        omitTermFreqAndPositions = fieldInfo.omitTermFreqAndPositions;
      }

      int df = appendPostings(termsConsumer, match, matchSize);		  // add new TermInfo

      checkAbort.work(df/3.0);

      while (matchSize > 0) {
        SegmentMergeInfo smi = match[--matchSize];
        if (smi.next())
          queue.add(smi);			  // restore queue
        else
          smi.close();				  // done with a segment
      }
    }
  }

  private byte[] payloadBuffer;
  private int[][] docMaps;
  int[][] getDocMaps() {
    return docMaps;
  }
  private int[] delCounts;
  int[] getDelCounts() {
    return delCounts;
  }

  /** Process postings from multiple segments all positioned on the
   *  same term. Writes out merged entries into freqOutput and
   *  the proxOutput streams.
   *
   * @param smis array of segments
   * @param n number of cells in the array actually occupied
   * @return number of documents across all segments where this term was found
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  private final int appendPostings(final FormatPostingsTermsConsumer termsConsumer, SegmentMergeInfo[] smis, int n)
        throws CorruptIndexException, IOException {

    final FormatPostingsDocsConsumer docConsumer = termsConsumer.addTerm(smis[0].term.text);
    int df = 0;
    for (int i = 0; i < n; i++) {
      SegmentMergeInfo smi = smis[i];
      TermPositions postings = smi.getPositions();
      assert postings != null;
      int base = smi.base;
      int[] docMap = smi.getDocMap();
      postings.seek(smi.termEnum);

      while (postings.next()) {
        df++;
        int doc = postings.doc();
        if (docMap != null)
          doc = docMap[doc];                      // map around deletions
        doc += base;                              // convert to merged space

        final int freq = postings.freq();
        final FormatPostingsPositionsConsumer posConsumer = docConsumer.addDoc(doc, freq);

        if (!omitTermFreqAndPositions) {
          for (int j = 0; j < freq; j++) {
            final int position = postings.nextPosition();
            final int payloadLength = postings.getPayloadLength();
            if (payloadLength > 0) {
              if (payloadBuffer == null || payloadBuffer.length < payloadLength)
                payloadBuffer = new byte[payloadLength];
              postings.getPayload(payloadBuffer, 0);
            }
            posConsumer.addPosition(position, payloadBuffer, 0, payloadLength);
          }
          posConsumer.finish();
        }
      }
    }
    docConsumer.finish();

    return df;
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
            output = directory.createOutput(segment + "." + IndexFileNames.NORMS_EXTENSION);
            output.writeBytes(NORMS_HEADER,NORMS_HEADER.length);
          }
          for (Iterator iter = readers.iterator(); iter.hasNext();) {
            IndexReader reader = (IndexReader) iter.next();
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
