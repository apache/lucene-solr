package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.util.Vector;

/**
 * Writer works by opening a document and then opening the fields within the document and then
 * writing out the vectors for each field.
 * 
 * Rough usage:
 *
 <CODE>
 for each document
 {
 writer.openDocument();
 for each field on the document
 {
 writer.openField(field);
 for all of the terms
 {
 writer.addTerm(...)
 }
 writer.closeField
 }
 writer.closeDocument()    
 }
 </CODE>
 *
 * @version $Id$
 * 
 */
final class TermVectorsWriter {
  static final byte STORE_POSITIONS_WITH_TERMVECTOR = 0x1;
  static final byte STORE_OFFSET_WITH_TERMVECTOR = 0x2;
  
  static final int FORMAT_VERSION = 2;
  //The size in bytes that the FORMAT_VERSION will take up at the beginning of each file 
  static final int FORMAT_SIZE = 4;
  
  static final String TVX_EXTENSION = ".tvx";
  static final String TVD_EXTENSION = ".tvd";
  static final String TVF_EXTENSION = ".tvf";
  
  private IndexOutput tvx = null, tvd = null, tvf = null;
  private Vector fields = null;
  private Vector terms = null;
  private FieldInfos fieldInfos;

  private TVField currentField = null;
  private long currentDocPointer = -1;

  public TermVectorsWriter(Directory directory, String segment,
                           FieldInfos fieldInfos)
    throws IOException {
    // Open files for TermVector storage
    tvx = directory.createOutput(segment + TVX_EXTENSION);
    tvx.writeInt(FORMAT_VERSION);
    tvd = directory.createOutput(segment + TVD_EXTENSION);
    tvd.writeInt(FORMAT_VERSION);
    tvf = directory.createOutput(segment + TVF_EXTENSION);
    tvf.writeInt(FORMAT_VERSION);

    this.fieldInfos = fieldInfos;
    fields = new Vector(fieldInfos.size());
    terms = new Vector();
  }


  public final void openDocument()
          throws IOException {
    closeDocument();
    currentDocPointer = tvd.getFilePointer();
  }


  public final void closeDocument()
          throws IOException {
    if (isDocumentOpen()) {
      closeField();
      writeDoc();
      fields.clear();
      currentDocPointer = -1;
    }
  }


  public final boolean isDocumentOpen() {
    return currentDocPointer != -1;
  }


  /** Start processing a field. This can be followed by a number of calls to
   *  addTerm, and a final call to closeField to indicate the end of
   *  processing of this field. If a field was previously open, it is
   *  closed automatically.
   */
  public final void openField(String field) throws IOException {
    FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
    openField(fieldInfo.number, fieldInfo.storePositionWithTermVector, fieldInfo.storeOffsetWithTermVector);
  }
  
  private void openField(int fieldNumber, boolean storePositionWithTermVector, 
      boolean storeOffsetWithTermVector) throws IOException{
    if (!isDocumentOpen()) 
      throw new IllegalStateException("Cannot open field when no document is open.");
    closeField();
    currentField = new TVField(fieldNumber, storePositionWithTermVector, storeOffsetWithTermVector);
  }

  /** Finished processing current field. This should be followed by a call to
   *  openField before future calls to addTerm.
   */
  public final void closeField()
          throws IOException {
    if (isFieldOpen()) {
      /* DEBUG */
      //System.out.println("closeField()");
      /* DEBUG */

      // save field and terms
      writeField();
      fields.add(currentField);
      terms.clear();
      currentField = null;
    }
  }

  /** Return true if a field is currently open. */
  public final boolean isFieldOpen() {
    return currentField != null;
  }

  /** Add term to the field's term vector. Fieldable must already be open.
   *  Terms should be added in
   *  increasing order of terms, one call per unique termNum. ProxPointer
   *  is a pointer into the TermPosition file (prx). Freq is the number of
   *  times this term appears in this field, in this document.
   * @throws IllegalStateException if document or field is not open
   */
  public final void addTerm(String termText, int freq) {
    addTerm(termText, freq, null, null);
  }
  
  public final void addTerm(String termText, int freq, int [] positions, TermVectorOffsetInfo [] offsets)
  {
    if (!isDocumentOpen()) 
      throw new IllegalStateException("Cannot add terms when document is not open");
    if (!isFieldOpen()) 
      throw new IllegalStateException("Cannot add terms when field is not open");
    
    addTermInternal(termText, freq, positions, offsets);
  }

  private final void addTermInternal(String termText, int freq, int [] positions, TermVectorOffsetInfo [] offsets) {
    TVTerm term = new TVTerm();
    term.termText = termText;
    term.freq = freq;
    term.positions = positions;
    term.offsets = offsets;
    terms.add(term);
  }

  /**
   * Add a complete document specified by all its term vectors. If document has no
   * term vectors, add value for tvx.
   * 
   * @param vectors
   * @throws IOException
   */
  public final void addAllDocVectors(TermFreqVector[] vectors)
      throws IOException {
    openDocument();

    if (vectors != null) {
      for (int i = 0; i < vectors.length; i++) {
        boolean storePositionWithTermVector = false;
        boolean storeOffsetWithTermVector = false;

        try {

          TermPositionVector tpVector = (TermPositionVector) vectors[i];

          if (tpVector.size() > 0 && tpVector.getTermPositions(0) != null)
            storePositionWithTermVector = true;
          if (tpVector.size() > 0 && tpVector.getOffsets(0) != null)
            storeOffsetWithTermVector = true;

          FieldInfo fieldInfo = fieldInfos.fieldInfo(tpVector.getField());
          openField(fieldInfo.number, storePositionWithTermVector, storeOffsetWithTermVector);

          for (int j = 0; j < tpVector.size(); j++)
            addTermInternal(tpVector.getTerms()[j], tpVector.getTermFrequencies()[j], tpVector.getTermPositions(j),
                tpVector.getOffsets(j));

          closeField();

        } catch (ClassCastException ignore) {

          TermFreqVector tfVector = vectors[i];

          FieldInfo fieldInfo = fieldInfos.fieldInfo(tfVector.getField());
          openField(fieldInfo.number, storePositionWithTermVector, storeOffsetWithTermVector);

          for (int j = 0; j < tfVector.size(); j++)
            addTermInternal(tfVector.getTerms()[j], tfVector.getTermFrequencies()[j], null, null);

          closeField();

        }
      }
    }

    closeDocument();
  }
  
  /** Close all streams. */
  final void close() throws IOException {
    try {
      closeDocument();
    } finally {
      // make an effort to close all streams we can but remember and re-throw
      // the first exception encountered in this process
      IOException keep = null;
      if (tvx != null)
        try {
          tvx.close();
        } catch (IOException e) {
          if (keep == null) keep = e;
        }
      if (tvd != null)
        try {
          tvd.close();
        } catch (IOException e) {
          if (keep == null) keep = e;
        }
      if (tvf != null)
        try {
          tvf.close();
        } catch (IOException e) {
          if (keep == null) keep = e;
        }
      if (keep != null) throw (IOException) keep.fillInStackTrace();
    }
  }

  

  private void writeField() throws IOException {
    // remember where this field is written
    currentField.tvfPointer = tvf.getFilePointer();
    //System.out.println("Fieldable Pointer: " + currentField.tvfPointer);
    
    final int size = terms.size();
    tvf.writeVInt(size);
    
    boolean storePositions = currentField.storePositions;
    boolean storeOffsets = currentField.storeOffsets;
    byte bits = 0x0;
    if (storePositions) 
      bits |= STORE_POSITIONS_WITH_TERMVECTOR;
    if (storeOffsets) 
      bits |= STORE_OFFSET_WITH_TERMVECTOR;
    tvf.writeByte(bits);
    
    String lastTermText = "";
    for (int i = 0; i < size; i++) {
      TVTerm term = (TVTerm) terms.elementAt(i);
      int start = StringHelper.stringDifference(lastTermText, term.termText);
      int length = term.termText.length() - start;
      tvf.writeVInt(start);       // write shared prefix length
      tvf.writeVInt(length);        // write delta length
      tvf.writeChars(term.termText, start, length);  // write delta chars
      tvf.writeVInt(term.freq);
      lastTermText = term.termText;
      
      if(storePositions){
        if(term.positions == null)
          throw new IllegalStateException("Trying to write positions that are null!");
        
        // use delta encoding for positions
        int position = 0;
        for (int j = 0; j < term.freq; j++){
          tvf.writeVInt(term.positions[j] - position);
          position = term.positions[j];
        }
      }
      
      if(storeOffsets){
        if(term.offsets == null)
          throw new IllegalStateException("Trying to write offsets that are null!");
        
        // use delta encoding for offsets
        int position = 0;
        for (int j = 0; j < term.freq; j++) {
          tvf.writeVInt(term.offsets[j].getStartOffset() - position);
          tvf.writeVInt(term.offsets[j].getEndOffset() - term.offsets[j].getStartOffset()); //Save the diff between the two.
          position = term.offsets[j].getEndOffset();
        }
      }
    }
  }

  private void writeDoc() throws IOException {
    if (isFieldOpen()) 
      throw new IllegalStateException("Field is still open while writing document");
    //System.out.println("Writing doc pointer: " + currentDocPointer);
    // write document index record
    tvx.writeLong(currentDocPointer);

    // write document data record
    final int size = fields.size();

    // write the number of fields
    tvd.writeVInt(size);

    // write field numbers
    for (int i = 0; i < size; i++) {
      TVField field = (TVField) fields.elementAt(i);
      tvd.writeVInt(field.number);
    }

    // write field pointers
    long lastFieldPointer = 0;
    for (int i = 0; i < size; i++) {
      TVField field = (TVField) fields.elementAt(i);
      tvd.writeVLong(field.tvfPointer - lastFieldPointer);
      lastFieldPointer = field.tvfPointer;
    }
    //System.out.println("After writing doc pointer: " + tvx.getFilePointer());
  }


  private static class TVField {
    int number;
    long tvfPointer = 0;
    boolean storePositions = false;
    boolean storeOffsets = false;
    TVField(int number, boolean storePos, boolean storeOff) {
      this.number = number;
      storePositions = storePos;
      storeOffsets = storeOff;
    }
  }

  private static class TVTerm {
    String termText;
    int freq = 0;
    int positions[] = null;
    TermVectorOffsetInfo [] offsets = null;
  }


}
