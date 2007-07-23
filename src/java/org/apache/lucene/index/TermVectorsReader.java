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

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * @version $Id$
 */
class TermVectorsReader implements Cloneable {
  private FieldInfos fieldInfos;

  private IndexInput tvx;
  private IndexInput tvd;
  private IndexInput tvf;
  private int size;

  // The docID offset where our docs begin in the index
  // file.  This will be 0 if we have our own private file.
  private int docStoreOffset;
  
  private int tvdFormat;
  private int tvfFormat;

  TermVectorsReader(Directory d, String segment, FieldInfos fieldInfos)
    throws CorruptIndexException, IOException {
    this(d, segment, fieldInfos, BufferedIndexInput.BUFFER_SIZE);
  }

  TermVectorsReader(Directory d, String segment, FieldInfos fieldInfos, int readBufferSize)
    throws CorruptIndexException, IOException {
    this(d, segment, fieldInfos, BufferedIndexInput.BUFFER_SIZE, -1, 0);
  }
    
  TermVectorsReader(Directory d, String segment, FieldInfos fieldInfos, int readBufferSize, int docStoreOffset, int size)
    throws CorruptIndexException, IOException {
    if (d.fileExists(segment + TermVectorsWriter.TVX_EXTENSION)) {
      tvx = d.openInput(segment + TermVectorsWriter.TVX_EXTENSION, readBufferSize);
      checkValidFormat(tvx);
      tvd = d.openInput(segment + TermVectorsWriter.TVD_EXTENSION, readBufferSize);
      tvdFormat = checkValidFormat(tvd);
      tvf = d.openInput(segment + TermVectorsWriter.TVF_EXTENSION, readBufferSize);
      tvfFormat = checkValidFormat(tvf);
      if (-1 == docStoreOffset) {
        this.docStoreOffset = 0;
        this.size = (int) (tvx.length() >> 3);
      } else {
        this.docStoreOffset = docStoreOffset;
        this.size = size;
        // Verify the file is long enough to hold all of our
        // docs
        assert ((int) (tvx.length()/8)) >= size + docStoreOffset;
      }
    }

    this.fieldInfos = fieldInfos;
  }
  
  private int checkValidFormat(IndexInput in) throws CorruptIndexException, IOException
  {
    int format = in.readInt();
    if (format > TermVectorsWriter.FORMAT_VERSION)
    {
      throw new CorruptIndexException("Incompatible format version: " + format + " expected " 
                                      + TermVectorsWriter.FORMAT_VERSION + " or less");
    }
    return format;
  }

  void close() throws IOException {
  	// make all effort to close up. Keep the first exception
  	// and throw it as a new one.
  	IOException keep = null;
  	if (tvx != null) try { tvx.close(); } catch (IOException e) { if (keep == null) keep = e; }
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
      tvx.seek(((docNum + docStoreOffset) * 8L) + TermVectorsWriter.FORMAT_SIZE);
      //System.out.println("TVX Pointer: " + tvx.getFilePointer());
      long position = tvx.readLong();

      tvd.seek(position);
      int fieldCount = tvd.readVInt();
      //System.out.println("Num Fields: " + fieldCount);
      // There are only a few fields per document. We opt for a full scan
      // rather then requiring that they be ordered. We need to read through
      // all of the fields anyway to get to the tvf pointers.
      int number = 0;
      int found = -1;
      for (int i = 0; i < fieldCount; i++) {
        if(tvdFormat == TermVectorsWriter.FORMAT_VERSION)
          number = tvd.readVInt();
        else
          number += tvd.readVInt();

        if (number == fieldNumber)
          found = i;
      }

      // This field, although valid in the segment, was not found in this
      // document
      if (found != -1) {
        // Compute position in the tvf file
        position = 0;
        for (int i = 0; i <= found; i++)
          position += tvd.readVLong();

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
      tvx.seek(((docNum + docStoreOffset) * 8L) + TermVectorsWriter.FORMAT_SIZE);
      long position = tvx.readLong();

      tvd.seek(position);
      int fieldCount = tvd.readVInt();

      // No fields are vectorized for this document
      if (fieldCount != 0) {
        int number = 0;
        String[] fields = new String[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
          if(tvdFormat == TermVectorsWriter.FORMAT_VERSION)
            number = tvd.readVInt();
          else
            number += tvd.readVInt();

          fields[i] = fieldInfos.fieldName(number);
        }

        // Compute position in the tvf file
        position = 0;
        long[] tvfPointers = new long[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
          position += tvd.readVLong();
          tvfPointers[i] = position;
        }

        result = readTermVectors(fields, tvfPointers);
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
      tvx.seek((docNumber * 8L) + TermVectorsWriter.FORMAT_SIZE);
      long position = tvx.readLong();

      tvd.seek(position);
      int fieldCount = tvd.readVInt();

      // No fields are vectorized for this document
      if (fieldCount != 0) {
        int number = 0;
        String[] fields = new String[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
          if(tvdFormat == TermVectorsWriter.FORMAT_VERSION)
            number = tvd.readVInt();
          else
            number += tvd.readVInt();

          fields[i] = fieldInfos.fieldName(number);
        }

        // Compute position in the tvf file
        position = 0;
        long[] tvfPointers = new long[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
          position += tvd.readVLong();
          tvfPointers[i] = position;
        }

        readTermVectors(fields, tvfPointers, mapper);
      }
    } else {
      //System.out.println("No tvx file");
    }
  }


  private SegmentTermVector[] readTermVectors(String fields[], long tvfPointers[])
          throws IOException {
    SegmentTermVector res[] = new SegmentTermVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      ParallelArrayTermVectorMapper mapper = new ParallelArrayTermVectorMapper();
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
   * @return The TermVector located at that position
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
    
    if(tvfFormat == TermVectorsWriter.FORMAT_VERSION){
      byte bits = tvf.readByte();
      storePositions = (bits & TermVectorsWriter.STORE_POSITIONS_WITH_TERMVECTOR) != 0;
      storeOffsets = (bits & TermVectorsWriter.STORE_OFFSET_WITH_TERMVECTOR) != 0;
    }
    else{
      tvf.readVInt();
      storePositions = false;
      storeOffsets = false;
    }
    mapper.setExpectations(field, numTerms, storeOffsets, storePositions);
    int start = 0;
    int deltaLength = 0;
    int totalLength = 0;
    char [] buffer = new char[10];    // init the buffer with a length of 10 character
    char[] previousBuffer = {};
    
    for (int i = 0; i < numTerms; i++) {
      start = tvf.readVInt();
      deltaLength = tvf.readVInt();
      totalLength = start + deltaLength;
      if (buffer.length < totalLength) {  // increase buffer
        buffer = null;    // give a hint to garbage collector
        buffer = new char[totalLength];
        
        if (start > 0)  // just copy if necessary
          System.arraycopy(previousBuffer, 0, buffer, 0, start);
      }
      
      tvf.readChars(buffer, start, deltaLength);
      String term = new String(buffer, 0, totalLength);
      previousBuffer = buffer;
      int freq = tvf.readVInt();
      int [] positions = null;
      if (storePositions) { //read in the positions
        //does the mapper even care about positions?
        if (mapper.isIgnoringPositions() == false) {
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
        if (mapper.isIgnoringOffsets() == false) {
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



  protected Object clone() {
    
    if (tvx == null || tvd == null || tvf == null)
      return null;
    
    TermVectorsReader clone = null;
    try {
      clone = (TermVectorsReader) super.clone();
    } catch (CloneNotSupportedException e) {}

    clone.tvx = (IndexInput) tvx.clone();
    clone.tvd = (IndexInput) tvd.clone();
    clone.tvf = (IndexInput) tvf.clone();
    
    return clone;
  }



}

/**
 * Models the existing parallel array structure
 */
class ParallelArrayTermVectorMapper extends TermVectorMapper
{

  private int numTerms;
  private String[] terms;
  private int[] termFreqs;
  private int positions[][] = null;
  private TermVectorOffsetInfo offsets[][] = null;
  private int currentPosition;
  private boolean storingOffsets;
  private boolean storingPositions;
  private String field;

  public void setExpectations(String field, int numTerms, boolean storeOffsets, boolean storePositions) {
    this.numTerms = numTerms;
    this.field = field;
    terms = new String[numTerms];
    termFreqs = new int[numTerms];
    this.storingOffsets = storeOffsets;
    this.storingPositions = storePositions;
    if(storePositions)
      this.positions = new int[numTerms][];
    if(storeOffsets)
      this.offsets = new TermVectorOffsetInfo[numTerms][];
  }

  public void map(String term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions) {
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
   * @return
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