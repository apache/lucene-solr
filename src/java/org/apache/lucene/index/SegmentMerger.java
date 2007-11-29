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

import java.util.Vector;
import java.util.Iterator;
import java.util.Collection;
import java.io.IOException;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IndexInput;

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

  private Vector readers = new Vector();
  private FieldInfos fieldInfos;
  
  private int mergedDocs;

  // Whether we should merge doc stores (stored fields and
  // vectors files).  When all segments we are merging
  // already share the same doc store files, we don't need
  // to merge the doc stores.
  private boolean mergeDocStores;

  /** Maximum number of contiguous documents to bulk-copy
      when merging stored fields */
  private final static int MAX_RAW_MERGE_DOCS = 16384;

  /** This ctor used only by test code.
   * 
   * @param dir The Directory to merge the other segments into
   * @param name The name of the new segment
   */
  SegmentMerger(Directory dir, String name) {
    directory = dir;
    segment = name;
  }

  SegmentMerger(IndexWriter writer, String name) {
    directory = writer.getDirectory();
    segment = name;
    termIndexInterval = writer.getTermIndexInterval();
  }

  /**
   * Add an IndexReader to the collection of readers that are to be merged
   * @param reader
   */
  final void add(IndexReader reader) {
    readers.addElement(reader);
  }

  /**
   * 
   * @param i The index of the reader to return
   * @return The ith reader to be merged
   */
  final IndexReader segmentReader(int i) {
    return (IndexReader) readers.elementAt(i);
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
    for (int i = 0; i < readers.size(); i++) {  // close readers
      IndexReader reader = (IndexReader) readers.elementAt(i);
      reader.close();
    }
  }

  final Vector createCompoundFile(String fileName)
          throws IOException {
    CompoundFileWriter cfsWriter =
            new CompoundFileWriter(directory, fileName);

    Vector files =
      new Vector(IndexFileNames.COMPOUND_EXTENSIONS.length + 1);    
    
    // Basic files
    for (int i = 0; i < IndexFileNames.COMPOUND_EXTENSIONS.length; i++) {
      String ext = IndexFileNames.COMPOUND_EXTENSIONS[i];
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

  private void addIndexed(IndexReader reader, FieldInfos fieldInfos, Collection names, boolean storeTermVectors, boolean storePositionWithTermVector,
                         boolean storeOffsetWithTermVector, boolean storePayloads) throws IOException {
    Iterator i = names.iterator();
    while (i.hasNext()) {
      String field = (String)i.next();
      fieldInfos.add(field, true, storeTermVectors, storePositionWithTermVector, storeOffsetWithTermVector, !reader.hasNorms(field), storePayloads);
    }
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
      final SegmentReader sr = (SegmentReader) readers.elementAt(readers.size()-1);
      fieldInfos = (FieldInfos) sr.fieldInfos.clone();
    } else {
      fieldInfos = new FieldInfos();		  // merge field names
    }

    for (int i = 0; i < readers.size(); i++) {
      IndexReader reader = (IndexReader) readers.elementAt(i);
      if (reader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) reader;
        for (int j = 0; j < segmentReader.getFieldInfos().size(); j++) {
          FieldInfo fi = segmentReader.getFieldInfos().fieldInfo(j);
          fieldInfos.add(fi.name, fi.isIndexed, fi.storeTermVector, fi.storePositionWithTermVector, fi.storeOffsetWithTermVector, !reader.hasNorms(fi.name), fi.storePayloads);
        }
      } else {
        addIndexed(reader, fieldInfos, reader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_POSITION_OFFSET), true, true, true, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_POSITION), true, true, false, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_OFFSET), true, false, true, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(IndexReader.FieldOption.TERMVECTOR), true, false, false, false);
        addIndexed(reader, fieldInfos, reader.getFieldNames(IndexReader.FieldOption.STORES_PAYLOADS), false, false, false, true);
        addIndexed(reader, fieldInfos, reader.getFieldNames(IndexReader.FieldOption.INDEXED), false, false, false, false);
        fieldInfos.add(reader.getFieldNames(IndexReader.FieldOption.UNINDEXED), false);
      }
    }
    fieldInfos.write(directory, segment + ".fnm");

    int docCount = 0;

    if (mergeDocStores) {

      // If the i'th reader is a SegmentReader and has
      // identical fieldName -> number mapping, then this
      // array will be non-null at position i:
      SegmentReader[] matchingSegmentReaders = new SegmentReader[readers.size()];

      for (int i = 0; i < readers.size(); i++) {
        IndexReader reader = (IndexReader) readers.elementAt(i);
        boolean same = reader.getFieldNames(IndexReader.FieldOption.ALL).size() == fieldInfos.size() && reader instanceof SegmentReader;
        if (same) {
          SegmentReader segmentReader = (SegmentReader) reader;
          for (int j = 0; same && j < fieldInfos.size(); j++)
            same = fieldInfos.fieldName(j).equals(segmentReader.getFieldInfos().fieldName(j));
          if (same)
            matchingSegmentReaders[i] = segmentReader;
        }
      }
	
      // Used for bulk-reading raw bytes for stored fields
      final int[] rawDocLengths = new int[MAX_RAW_MERGE_DOCS];

      // merge field values
      final FieldsWriter fieldsWriter = new FieldsWriter(directory, segment, fieldInfos);

      // for merging we don't want to compress/uncompress the data, so to tell the FieldsReader that we're
      // in  merge mode, we use this FieldSelector
      FieldSelector fieldSelectorMerge = new FieldSelector() {
          public FieldSelectorResult accept(String fieldName) {
            return FieldSelectorResult.LOAD_FOR_MERGE;
          }        
        };

      try {
        for (int i = 0; i < readers.size(); i++) {
          final IndexReader reader = (IndexReader) readers.elementAt(i);
          final SegmentReader matchingSegmentReader = matchingSegmentReaders[i];
          final FieldsReader matchingFieldsReader;
          if (matchingSegmentReader != null)
            matchingFieldsReader = matchingSegmentReader.getFieldsReader();
          else
            matchingFieldsReader = null;
          final int maxDoc = reader.maxDoc();
          for (int j = 0; j < maxDoc;) {
            if (!reader.isDeleted(j)) { // skip deleted docs
              if (matchingSegmentReader != null) {
                // We can optimize this case (doing a bulk
                // byte copy) since the field numbers are
                // identical
                int start = j;
                int numDocs = 0;
                do {
                  j++;
                  numDocs++;
                } while(j < maxDoc && !matchingSegmentReader.isDeleted(j) && numDocs < MAX_RAW_MERGE_DOCS);

                IndexInput stream = matchingFieldsReader.rawDocs(rawDocLengths, start, numDocs);
                fieldsWriter.addRawDocuments(stream, rawDocLengths, numDocs);
                docCount += numDocs;
              } else {
                fieldsWriter.addDocument(reader.document(j, fieldSelectorMerge));
                j++;
                docCount++;
              }
            } else
              j++;
          }
        }
      } finally {
        fieldsWriter.close();
      }

    } else
      // If we are skipping the doc stores, that means there
      // are no deletions in any of these segments, so we
      // just sum numDocs() of each segment to get total docCount
      for (int i = 0; i < readers.size(); i++)
        docCount += ((IndexReader) readers.elementAt(i)).numDocs();

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
      for (int r = 0; r < readers.size(); r++) {
        IndexReader reader = (IndexReader) readers.elementAt(r);
        int maxDoc = reader.maxDoc();
        for (int docNum = 0; docNum < maxDoc; docNum++) {
          // skip deleted docs
          if (reader.isDeleted(docNum)) 
            continue;
          termVectorsWriter.addAllDocVectors(reader.getTermFreqVectors(docNum));
        }
      }
    } finally {
      termVectorsWriter.close();
    }
  }

  private IndexOutput freqOutput = null;
  private IndexOutput proxOutput = null;
  private TermInfosWriter termInfosWriter = null;
  private int skipInterval;
  private int maxSkipLevels;
  private SegmentMergeQueue queue = null;
  private DefaultSkipListWriter skipListWriter = null;

  private final void mergeTerms() throws CorruptIndexException, IOException {
    try {
      freqOutput = directory.createOutput(segment + ".frq");
      proxOutput = directory.createOutput(segment + ".prx");
      termInfosWriter =
              new TermInfosWriter(directory, segment, fieldInfos,
                                  termIndexInterval);
      skipInterval = termInfosWriter.skipInterval;
      maxSkipLevels = termInfosWriter.maxSkipLevels;
      skipListWriter = new DefaultSkipListWriter(skipInterval, maxSkipLevels, mergedDocs, freqOutput, proxOutput);
      queue = new SegmentMergeQueue(readers.size());

      mergeTermInfos();

    } finally {
      if (freqOutput != null) freqOutput.close();
      if (proxOutput != null) proxOutput.close();
      if (termInfosWriter != null) termInfosWriter.close();
      if (queue != null) queue.close();
    }
  }

  private final void mergeTermInfos() throws CorruptIndexException, IOException {
    int base = 0;
    for (int i = 0; i < readers.size(); i++) {
      IndexReader reader = (IndexReader) readers.elementAt(i);
      TermEnum termEnum = reader.terms();
      SegmentMergeInfo smi = new SegmentMergeInfo(base, termEnum, reader);
      base += reader.numDocs();
      if (smi.next())
        queue.put(smi);				  // initialize queue
      else
        smi.close();
    }

    SegmentMergeInfo[] match = new SegmentMergeInfo[readers.size()];

    while (queue.size() > 0) {
      int matchSize = 0;			  // pop matching terms
      match[matchSize++] = (SegmentMergeInfo) queue.pop();
      Term term = match[0].term;
      SegmentMergeInfo top = (SegmentMergeInfo) queue.top();

      while (top != null && term.compareTo(top.term) == 0) {
        match[matchSize++] = (SegmentMergeInfo) queue.pop();
        top = (SegmentMergeInfo) queue.top();
      }

      mergeTermInfo(match, matchSize);		  // add new TermInfo

      while (matchSize > 0) {
        SegmentMergeInfo smi = match[--matchSize];
        if (smi.next())
          queue.put(smi);			  // restore queue
        else
          smi.close();				  // done with a segment
      }
    }
  }

  private final TermInfo termInfo = new TermInfo(); // minimize consing

  /** Merge one term found in one or more segments. The array <code>smis</code>
   *  contains segments that are positioned at the same term. <code>N</code>
   *  is the number of cells in the array actually occupied.
   *
   * @param smis array of segments
   * @param n number of cells in the array actually occupied
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  private final void mergeTermInfo(SegmentMergeInfo[] smis, int n)
          throws CorruptIndexException, IOException {
    long freqPointer = freqOutput.getFilePointer();
    long proxPointer = proxOutput.getFilePointer();

    int df = appendPostings(smis, n);		  // append posting data

    long skipPointer = skipListWriter.writeSkip(freqOutput);

    if (df > 0) {
      // add an entry to the dictionary with pointers to prox and freq files
      termInfo.set(df, freqPointer, proxPointer, (int) (skipPointer - freqPointer));
      termInfosWriter.add(smis[0].term, termInfo);
    }
  }
  
  private byte[] payloadBuffer = null;

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
  private final int appendPostings(SegmentMergeInfo[] smis, int n)
          throws CorruptIndexException, IOException {
    int lastDoc = 0;
    int df = 0;					  // number of docs w/ term
    skipListWriter.resetSkip();
    boolean storePayloads = fieldInfos.fieldInfo(smis[0].term.field).storePayloads;
    int lastPayloadLength = -1;   // ensures that we write the first length
    for (int i = 0; i < n; i++) {
      SegmentMergeInfo smi = smis[i];
      TermPositions postings = smi.getPositions();
      assert postings != null;
      int base = smi.base;
      int[] docMap = smi.getDocMap();
      postings.seek(smi.termEnum);
      while (postings.next()) {
        int doc = postings.doc();
        if (docMap != null)
          doc = docMap[doc];                      // map around deletions
        doc += base;                              // convert to merged space

        if (doc < 0 || (df > 0 && doc <= lastDoc))
          throw new CorruptIndexException("docs out of order (" + doc +
              " <= " + lastDoc + " )");

        df++;

        if ((df % skipInterval) == 0) {
          skipListWriter.setSkipData(lastDoc, storePayloads, lastPayloadLength);
          skipListWriter.bufferSkip(df);
        }

        int docCode = (doc - lastDoc) << 1;	  // use low bit to flag freq=1
        lastDoc = doc;

        int freq = postings.freq();
        if (freq == 1) {
          freqOutput.writeVInt(docCode | 1);	  // write doc & freq=1
        } else {
          freqOutput.writeVInt(docCode);	  // write doc
          freqOutput.writeVInt(freq);		  // write frequency in doc
        }
        
        /** See {@link DocumentWriter#writePostings(Posting[], String) for 
         *  documentation about the encoding of positions and payloads
         */
        int lastPosition = 0;			  // write position deltas
        for (int j = 0; j < freq; j++) {
          int position = postings.nextPosition();
          int delta = position - lastPosition;
          if (storePayloads) {
            int payloadLength = postings.getPayloadLength();
            if (payloadLength == lastPayloadLength) {
              proxOutput.writeVInt(delta * 2);
            } else {
              proxOutput.writeVInt(delta * 2 + 1);
              proxOutput.writeVInt(payloadLength);
              lastPayloadLength = payloadLength;
            }
            if (payloadLength > 0) {
              if (payloadBuffer == null || payloadBuffer.length < payloadLength) {
                payloadBuffer = new byte[payloadLength];
              }
              postings.getPayload(payloadBuffer, 0);
              proxOutput.writeBytes(payloadBuffer, 0, payloadLength);
            }
          } else {
            proxOutput.writeVInt(delta);
          }
          lastPosition = position;
        }
      }
    }
    return df;
  }

  private void mergeNorms() throws IOException {
    byte[] normBuffer = null;
    IndexOutput output = null;
    try {
      for (int i = 0; i < fieldInfos.size(); i++) {
        FieldInfo fi = fieldInfos.fieldInfo(i);
        if (fi.isIndexed && !fi.omitNorms) {
          if (output == null) { 
            output = directory.createOutput(segment + "." + IndexFileNames.NORMS_EXTENSION);
            output.writeBytes(NORMS_HEADER,NORMS_HEADER.length);
          }
          for (int j = 0; j < readers.size(); j++) {
            IndexReader reader = (IndexReader) readers.elementAt(j);
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
