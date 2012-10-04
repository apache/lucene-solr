package org.apache.lucene.codecs.lucene3x;

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

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexFormatTooNewException;

/**
 * @deprecated (4.0) No longer used with flex indexing, except for
 * reading old segments 
 * @lucene.experimental */

@Deprecated
final class SegmentTermEnum implements Cloneable,Closeable {
  private IndexInput input;
  FieldInfos fieldInfos;
  long size;
  long position = -1;

  // Changed strings to true utf8 with length-in-bytes not
  // length-in-chars
  public static final int FORMAT_VERSION_UTF8_LENGTH_IN_BYTES = -4;

  // NOTE: always change this if you switch to a new format!
  // whenever you add a new format, make it 1 smaller (negative version logic)!
  public static final int FORMAT_CURRENT = FORMAT_VERSION_UTF8_LENGTH_IN_BYTES;
  
  // when removing support for old versions, leave the last supported version here
  public static final int FORMAT_MINIMUM = FORMAT_VERSION_UTF8_LENGTH_IN_BYTES;

  private TermBuffer termBuffer = new TermBuffer();
  private TermBuffer prevBuffer = new TermBuffer();
  private TermBuffer scanBuffer = new TermBuffer(); // used for scanning

  TermInfo termInfo = new TermInfo();

  private int format;
  private boolean isIndex = false;
  long indexPointer = 0;
  int indexInterval;
  int skipInterval;
  int newSuffixStart;
  int maxSkipLevels;
  private boolean first = true;

  SegmentTermEnum(IndexInput i, FieldInfos fis, boolean isi)
          throws CorruptIndexException, IOException {
    input = i;
    fieldInfos = fis;
    isIndex = isi;
    maxSkipLevels = 1; // use single-level skip lists for formats > -3 
    
    int firstInt = input.readInt();
    if (firstInt >= 0) {
      // original-format file, without explicit format version number
      format = 0;
      size = firstInt;

      // back-compatible settings
      indexInterval = 128;
      skipInterval = Integer.MAX_VALUE; // switch off skipTo optimization
    } else {
      // we have a format version number
      format = firstInt;

      // check that it is a format we can understand
      if (format > FORMAT_MINIMUM)
        throw new IndexFormatTooOldException(input, format, FORMAT_MINIMUM, FORMAT_CURRENT);
      if (format < FORMAT_CURRENT)
        throw new IndexFormatTooNewException(input, format, FORMAT_MINIMUM, FORMAT_CURRENT);

      size = input.readLong();                    // read the size
      
      indexInterval = input.readInt();
      skipInterval = input.readInt();
      maxSkipLevels = input.readInt();
      assert indexInterval > 0: "indexInterval=" + indexInterval + " is negative; must be > 0";
      assert skipInterval > 0: "skipInterval=" + skipInterval + " is negative; must be > 0";
    }
  }

  @Override
  protected SegmentTermEnum clone() {
    SegmentTermEnum clone = null;
    try {
      clone = (SegmentTermEnum) super.clone();
    } catch (CloneNotSupportedException e) {}

    clone.input = input.clone();
    clone.termInfo = new TermInfo(termInfo);

    clone.termBuffer = termBuffer.clone();
    clone.prevBuffer = prevBuffer.clone();
    clone.scanBuffer = new TermBuffer();

    return clone;
  }

  final void seek(long pointer, long p, Term t, TermInfo ti)
          throws IOException {
    input.seek(pointer);
    position = p;
    termBuffer.set(t);
    prevBuffer.reset();
    //System.out.println("  ste doSeek prev=" + prevBuffer.toTerm() + " this=" + this);
    termInfo.set(ti);
    first = p == -1;
  }

  /** Increments the enumeration to the next element.  True if one exists.*/
  public final boolean next() throws IOException {
    prevBuffer.set(termBuffer);
    //System.out.println("  ste setPrev=" + prev() + " this=" + this);

    if (position++ >= size - 1) {
      termBuffer.reset();
      //System.out.println("    EOF");
      return false;
    }

    termBuffer.read(input, fieldInfos);
    newSuffixStart = termBuffer.newSuffixStart;

    termInfo.docFreq = input.readVInt();    // read doc freq
    termInfo.freqPointer += input.readVLong();    // read freq pointer
    termInfo.proxPointer += input.readVLong();    // read prox pointer
    
    if (termInfo.docFreq >= skipInterval) 
      termInfo.skipOffset = input.readVInt();

    if (isIndex)
      indexPointer += input.readVLong();    // read index pointer

    //System.out.println("  ste ret term=" + term());
    return true;
  }

  /* Optimized scan, without allocating new terms. 
   *  Return number of invocations to next().
   *
   * NOTE: LUCENE-3183: if you pass Term("", "") here then this
   * will incorrectly return before positioning the enum,
   * and position will be -1; caller must detect this. */
  final int scanTo(Term term) throws IOException {
    scanBuffer.set(term);
    int count = 0;
    if (first) {
      // Always force initial next() in case term is
      // Term("", "")
      next();
      first = false;
      count++;
    }
    while (scanBuffer.compareTo(termBuffer) > 0 && next()) {
      count++;
    }
    return count;
  }

  /** Returns the current Term in the enumeration.
   Initially invalid, valid after next() called for the first time.*/
  public final Term term() {
    return termBuffer.toTerm();
  }

  /** Returns the previous Term enumerated. Initially null.*/
  final Term prev() {
    return prevBuffer.toTerm();
  }

  /** Returns the current TermInfo in the enumeration.
   Initially invalid, valid after next() called for the first time.*/
  final TermInfo termInfo() {
    return new TermInfo(termInfo);
  }

  /** Sets the argument to the current TermInfo in the enumeration.
   Initially invalid, valid after next() called for the first time.*/
  final void termInfo(TermInfo ti) {
    ti.set(termInfo);
  }

  /** Returns the docFreq from the current TermInfo in the enumeration.
   Initially invalid, valid after next() called for the first time.*/
  public final int docFreq() {
    return termInfo.docFreq;
  }

  /* Returns the freqPointer from the current TermInfo in the enumeration.
    Initially invalid, valid after next() called for the first time.*/
  final long freqPointer() {
    return termInfo.freqPointer;
  }

  /* Returns the proxPointer from the current TermInfo in the enumeration.
    Initially invalid, valid after next() called for the first time.*/
  final long proxPointer() {
    return termInfo.proxPointer;
  }

  /** Closes the enumeration to further activity, freeing resources. */
  public final void close() throws IOException {
    input.close();
  }
}
