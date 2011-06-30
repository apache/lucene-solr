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

import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;

class TermVectorsReader implements Cloneable {

  // NOTE: if you make a new format, it must be larger than
  // the current format

  // Changed strings to UTF8 with length-in-bytes not length-in-chars
  static final int FORMAT_UTF8_LENGTH_IN_BYTES = 4;

  // NOTE: always change this if you switch to a new format!
  // whenever you add a new format, make it 1 larger (positive version logic)!
  static final int FORMAT_CURRENT = FORMAT_UTF8_LENGTH_IN_BYTES;
  
  // when removing support for old versions, leave the last supported version here
  static final int FORMAT_MINIMUM = FORMAT_UTF8_LENGTH_IN_BYTES;

  //The size in bytes that the FORMAT_VERSION will take up at the beginning of each file 
  static final int FORMAT_SIZE = 4;

  static final byte STORE_POSITIONS_WITH_TERMVECTOR = 0x1;
  static final byte STORE_OFFSET_WITH_TERMVECTOR = 0x2;
  
  private FieldInfos fieldInfos;

  private IndexInput tvx;
  private IndexInput tvd;
  private IndexInput tvf;
  private int size;
  private int numTotalDocs;

  // The docID offset where our docs begin in the index
  // file.  This will be 0 if we have our own private file.
  private int docStoreOffset;
  
  private final int format;

  TermVectorsReader(Directory d, String segment, FieldInfos fieldInfos, IOContext context)
    throws CorruptIndexException, IOException {
    this(d, segment, fieldInfos, context, -1, 0);
  }
    
  TermVectorsReader(Directory d, String segment, FieldInfos fieldInfos, IOContext context, int docStoreOffset, int size)
    throws CorruptIndexException, IOException {
    boolean success = false;

    try {
      String idxName = IndexFileNames.segmentFileName(segment, "", IndexFileNames.VECTORS_INDEX_EXTENSION);
      tvx = d.openInput(idxName, context);
      format = checkValidFormat(tvx, idxName);
      String fn = IndexFileNames.segmentFileName(segment, "", IndexFileNames.VECTORS_DOCUMENTS_EXTENSION);
      tvd = d.openInput(fn, context);
      final int tvdFormat = checkValidFormat(tvd, fn);
      fn = IndexFileNames.segmentFileName(segment, "", IndexFileNames.VECTORS_FIELDS_EXTENSION);
      tvf = d.openInput(fn, context);
      final int tvfFormat = checkValidFormat(tvf, fn);

      assert format == tvdFormat;
      assert format == tvfFormat;

      numTotalDocs = (int) (tvx.length() >> 4);

      if (-1 == docStoreOffset) {
        this.docStoreOffset = 0;
        this.size = numTotalDocs;
        assert size == 0 || numTotalDocs == size;
      } else {
        this.docStoreOffset = docStoreOffset;
        this.size = size;
        // Verify the file is long enough to hold all of our
        // docs
        assert numTotalDocs >= size + docStoreOffset: "numTotalDocs=" + numTotalDocs + " size=" + size + " docStoreOffset=" + docStoreOffset;
      }

      this.fieldInfos = fieldInfos;
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above. In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        close();
      }
    }
  }

  // Used for bulk copy when merging
  IndexInput getTvdStream() {
    return tvd;
  }

  // Used for bulk copy when merging
  IndexInput getTvfStream() {
    return tvf;
  }

  private void seekTvx(final int docNum) throws IOException {
    tvx.seek((docNum + docStoreOffset) * 16L + FORMAT_SIZE);
  }

  boolean canReadRawDocs() {
    // we can always read raw docs, unless the term vectors
    // didn't exist
    return format != 0;
  }

  /** Retrieve the length (in bytes) of the tvd and tvf
   *  entries for the next numDocs starting with
   *  startDocID.  This is used for bulk copying when
   *  merging segments, if the field numbers are
   *  congruent.  Once this returns, the tvf & tvd streams
   *  are seeked to the startDocID. */
  final void rawDocs(int[] tvdLengths, int[] tvfLengths, int startDocID, int numDocs) throws IOException {

    if (tvx == null) {
      Arrays.fill(tvdLengths, 0);
      Arrays.fill(tvfLengths, 0);
      return;
    }

    seekTvx(startDocID);

    long tvdPosition = tvx.readLong();
    tvd.seek(tvdPosition);

    long tvfPosition = tvx.readLong();
    tvf.seek(tvfPosition);

    long lastTvdPosition = tvdPosition;
    long lastTvfPosition = tvfPosition;

    int count = 0;
    while (count < numDocs) {
      final int docID = docStoreOffset + startDocID + count + 1;
      assert docID <= numTotalDocs;
      if (docID < numTotalDocs)  {
        tvdPosition = tvx.readLong();
        tvfPosition = tvx.readLong();
      } else {
        tvdPosition = tvd.length();
        tvfPosition = tvf.length();
        assert count == numDocs-1;
      }
      tvdLengths[count] = (int) (tvdPosition-lastTvdPosition);
      tvfLengths[count] = (int) (tvfPosition-lastTvfPosition);
      count++;
      lastTvdPosition = tvdPosition;
      lastTvfPosition = tvfPosition;
    }
  }

  private int checkValidFormat(IndexInput in, String fn) throws CorruptIndexException, IOException
  {
    int format = in.readInt();
    if (format < FORMAT_MINIMUM)
      throw new IndexFormatTooOldException(fn, format, FORMAT_MINIMUM, FORMAT_CURRENT);
    if (format > FORMAT_CURRENT)
      throw new IndexFormatTooNewException(fn, format, FORMAT_MINIMUM, FORMAT_CURRENT);
    return format;
  }

  void close() throws IOException {
    // make all effort to close up. Keep the first exception
    // and throw it as a new one.
    IOException keep = null;
    if (tvx != null) try { tvx.close(); } catch (IOException e) { keep = e; }
    if (tvd != null) try { tvd.close(); } catch (IOException e) { if (keep == null) keep = e; }
    if (tvf  != null) try {  tvf.close(); } catch (IOException e) { if (keep == null) keep = e; }
    if (keep != null) throw (IOException) keep.fillInStackTrace();
  }

  /**
   * 
   * @return The number of documents in the reader
   */
  int size() {
    return size;
  }

  public void get(int docNum, String field, TermVectorMapper mapper) throws IOException {
    if (tvx != null) {
      int fieldNumber = fieldInfos.fieldNumber(field);
      //We need to account for the FORMAT_SIZE at when seeking in the tvx
      //We don't need to do this in other seeks because we already have the
      // file pointer
      //that was written in another file
      seekTvx(docNum);
      //System.out.println("TVX Pointer: " + tvx.getFilePointer());
      long tvdPosition = tvx.readLong();

      tvd.seek(tvdPosition);
      int fieldCount = tvd.readVInt();
      //System.out.println("Num Fields: " + fieldCount);
      // There are only a few fields per document. We opt for a full scan
      // rather then requiring that they be ordered. We need to read through
      // all of the fields anyway to get to the tvf pointers.
      int number = 0;
      int found = -1;
      for (int i = 0; i < fieldCount; i++) {
        number = tvd.readVInt();
        if (number == fieldNumber)
          found = i;
      }

      // This field, although valid in the segment, was not found in this
      // document
      if (found != -1) {
        // Compute position in the tvf file
        long position = tvx.readLong();
        for (int i = 1; i <= found; i++)
          position += tvd.readVLong();

        mapper.setDocumentNumber(docNum);
        readTermVector(field, position, mapper);
      } else {
        //System.out.println("Fieldable not found");
      }
    } else {
      //System.out.println("No tvx file");
    }
  }



  /**
   * Retrieve the term vector for the given document and field
   * @param docNum The document number to retrieve the vector for
   * @param field The field within the document to retrieve
   * @return The TermFreqVector for the document and field or null if there is no termVector for this field.
   * @throws IOException if there is an error reading the term vector files
   */ 
  TermFreqVector get(int docNum, String field) throws IOException {
    // Check if no term vectors are available for this segment at all
    ParallelArrayTermVectorMapper mapper = new ParallelArrayTermVectorMapper();
    get(docNum, field, mapper);

    return mapper.materializeVector();
  }

  // Reads the String[] fields; you have to pre-seek tvd to
  // the right point
  private String[] readFields(int fieldCount) throws IOException {
    int number = 0;
    String[] fields = new String[fieldCount];

    for (int i = 0; i < fieldCount; i++) {
      number = tvd.readVInt();
      fields[i] = fieldInfos.fieldName(number);
    }

    return fields;
  }

  // Reads the long[] offsets into TVF; you have to pre-seek
  // tvx/tvd to the right point
  private long[] readTvfPointers(int fieldCount) throws IOException {
    // Compute position in the tvf file
    long position = tvx.readLong();

    long[] tvfPointers = new long[fieldCount];
    tvfPointers[0] = position;

    for (int i = 1; i < fieldCount; i++) {
      position += tvd.readVLong();
      tvfPointers[i] = position;
    }

    return tvfPointers;
  }

  /**
   * Return all term vectors stored for this document or null if the could not be read in.
   * 
   * @param docNum The document number to retrieve the vector for
   * @return All term frequency vectors
   * @throws IOException if there is an error reading the term vector files 
   */
  TermFreqVector[] get(int docNum) throws IOException {
    TermFreqVector[] result = null;
    if (tvx != null) {
      //We need to offset by
      seekTvx(docNum);
      long tvdPosition = tvx.readLong();

      tvd.seek(tvdPosition);
      int fieldCount = tvd.readVInt();

      // No fields are vectorized for this document
      if (fieldCount != 0) {
        final String[] fields = readFields(fieldCount);
        final long[] tvfPointers = readTvfPointers(fieldCount);
        result = readTermVectors(docNum, fields, tvfPointers);
      }
    } else {
      //System.out.println("No tvx file");
    }
    return result;
  }

  public void get(int docNumber, TermVectorMapper mapper) throws IOException {
    // Check if no term vectors are available for this segment at all
    if (tvx != null) {
      //We need to offset by

      seekTvx(docNumber);
      long tvdPosition = tvx.readLong();

      tvd.seek(tvdPosition);
      int fieldCount = tvd.readVInt();

      // No fields are vectorized for this document
      if (fieldCount != 0) {
        final String[] fields = readFields(fieldCount);
        final long[] tvfPointers = readTvfPointers(fieldCount);
        mapper.setDocumentNumber(docNumber);
        readTermVectors(fields, tvfPointers, mapper);
      }
    } else {
      //System.out.println("No tvx file");
    }
  }


  private SegmentTermVector[] readTermVectors(int docNum, String fields[], long tvfPointers[])
          throws IOException {
    SegmentTermVector res[] = new SegmentTermVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      ParallelArrayTermVectorMapper mapper = new ParallelArrayTermVectorMapper();
      mapper.setDocumentNumber(docNum);
      readTermVector(fields[i], tvfPointers[i], mapper);
      res[i] = (SegmentTermVector) mapper.materializeVector();
    }
    return res;
  }

  private void readTermVectors(String fields[], long tvfPointers[], TermVectorMapper mapper)
          throws IOException {
    for (int i = 0; i < fields.length; i++) {
      readTermVector(fields[i], tvfPointers[i], mapper);
    }
  }


  /**
   * 
   * @param field The field to read in
   * @param tvfPointer The pointer within the tvf file where we should start reading
   * @param mapper The mapper used to map the TermVector
   * @throws IOException
   */ 
  private void readTermVector(String field, long tvfPointer, TermVectorMapper mapper)
          throws IOException {

    // Now read the data from specified position
    //We don't need to offset by the FORMAT here since the pointer already includes the offset
    tvf.seek(tvfPointer);

    int numTerms = tvf.readVInt();
    //System.out.println("Num Terms: " + numTerms);
    // If no terms - return a constant empty termvector. However, this should never occur!
    if (numTerms == 0) 
      return;
    
    boolean storePositions;
    boolean storeOffsets;
    
    byte bits = tvf.readByte();
    storePositions = (bits & STORE_POSITIONS_WITH_TERMVECTOR) != 0;
    storeOffsets = (bits & STORE_OFFSET_WITH_TERMVECTOR) != 0;

    mapper.setExpectations(field, numTerms, storeOffsets, storePositions);
    int start = 0;
    int deltaLength = 0;
    int totalLength = 0;
    byte[] byteBuffer;

    // init the buffer
    byteBuffer = new byte[20];

    for (int i = 0; i < numTerms; i++) {
      start = tvf.readVInt();
      deltaLength = tvf.readVInt();
      totalLength = start + deltaLength;

      final BytesRef term = new BytesRef(totalLength);
      
      // Term stored as utf8 bytes
      if (byteBuffer.length < totalLength) {
        byteBuffer = ArrayUtil.grow(byteBuffer, totalLength);
      }
      tvf.readBytes(byteBuffer, start, deltaLength);
      System.arraycopy(byteBuffer, 0, term.bytes, 0, totalLength);
      term.length = totalLength;
      int freq = tvf.readVInt();
      int [] positions = null;
      if (storePositions) { //read in the positions
        //does the mapper even care about positions?
        if (!mapper.isIgnoringPositions()) {
          positions = new int[freq];
          int prevPosition = 0;
          for (int j = 0; j < freq; j++)
          {
            positions[j] = prevPosition + tvf.readVInt();
            prevPosition = positions[j];
          }
        } else {
          //we need to skip over the positions.  Since these are VInts, I don't believe there is anyway to know for sure how far to skip
          //
          for (int j = 0; j < freq; j++)
          {
            tvf.readVInt();
          }
        }
      }
      TermVectorOffsetInfo[] offsets = null;
      if (storeOffsets) {
        //does the mapper even care about offsets?
        if (!mapper.isIgnoringOffsets()) {
          offsets = new TermVectorOffsetInfo[freq];
          int prevOffset = 0;
          for (int j = 0; j < freq; j++) {
            int startOffset = prevOffset + tvf.readVInt();
            int endOffset = startOffset + tvf.readVInt();
            offsets[j] = new TermVectorOffsetInfo(startOffset, endOffset);
            prevOffset = endOffset;
          }
        } else {
          for (int j = 0; j < freq; j++){
            tvf.readVInt();
            tvf.readVInt();
          }
        }
      }
      mapper.map(term, freq, offsets, positions);
    }
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    
    final TermVectorsReader clone = (TermVectorsReader) super.clone();

    // These are null when a TermVectorsReader was created
    // on a segment that did not have term vectors saved
    if (tvx != null && tvd != null && tvf != null) {
      clone.tvx = (IndexInput) tvx.clone();
      clone.tvd = (IndexInput) tvd.clone();
      clone.tvf = (IndexInput) tvf.clone();
    }
    
    return clone;
  }
}


/**
 * Models the existing parallel array structure
 */
class ParallelArrayTermVectorMapper extends TermVectorMapper
{

  private BytesRef[] terms;
  private int[] termFreqs;
  private int positions[][];
  private TermVectorOffsetInfo offsets[][];
  private int currentPosition;
  private boolean storingOffsets;
  private boolean storingPositions;
  private String field;

  @Override
  public void setExpectations(String field, int numTerms, boolean storeOffsets, boolean storePositions) {
    this.field = field;
    terms = new BytesRef[numTerms];
    termFreqs = new int[numTerms];
    this.storingOffsets = storeOffsets;
    this.storingPositions = storePositions;
    if(storePositions)
      this.positions = new int[numTerms][];
    if(storeOffsets)
      this.offsets = new TermVectorOffsetInfo[numTerms][];
  }

  @Override
  public void map(BytesRef term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions) {
    terms[currentPosition] = term;
    termFreqs[currentPosition] = frequency;
    if (storingOffsets)
    {
      this.offsets[currentPosition] = offsets;
    }
    if (storingPositions)
    {
      this.positions[currentPosition] = positions; 
    }
    currentPosition++;
  }

  /**
   * Construct the vector
   * @return The {@link TermFreqVector} based on the mappings.
   */
  public TermFreqVector materializeVector() {
    SegmentTermVector tv = null;
    if (field != null && terms != null) {
      if (storingPositions || storingOffsets) {
        tv = new SegmentTermPositionVector(field, terms, termFreqs, positions, offsets);
      } else {
        tv = new SegmentTermVector(field, terms, termFreqs);
      }
    }
    return tv;
  }
}
