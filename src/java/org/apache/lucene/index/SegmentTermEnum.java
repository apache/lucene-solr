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

import java.io.IOException;
import org.apache.lucene.store.InputStream;

final class SegmentTermEnum extends TermEnum implements Cloneable {
  private InputStream input;
  private FieldInfos fieldInfos;
  long size;
  long position = -1;

  private Term term = new Term("", "");
  private TermInfo termInfo = new TermInfo();

  private int format;
  private boolean isIndex = false;
  long indexPointer = 0;
  int indexInterval;
  int skipInterval;
  Term prev;

  private char[] buffer = {};

  SegmentTermEnum(InputStream i, FieldInfos fis, boolean isi)
          throws IOException {
    input = i;
    fieldInfos = fis;
    isIndex = isi;

    int firstInt = input.readInt();
    if (firstInt >= 0) {
      // original-format file, without explicit format version number
      format = 0;
      size = firstInt;

      // back-compatible settings
      indexInterval = 128;
      skipInterval = Integer.MAX_VALUE;

    } else {
      // we have a format version number
      format = firstInt;

      // check that it is a format we can understand
      if (format < TermInfosWriter.FORMAT)
        throw new IOException("Unknown format version:" + format);

      size = input.readLong();                    // read the size

      if (!isIndex) {
        indexInterval = input.readInt();
        skipInterval = input.readInt();
      }
    }

  }

  protected Object clone() {
    SegmentTermEnum clone = null;
    try {
      clone = (SegmentTermEnum) super.clone();
    } catch (CloneNotSupportedException e) {}

    clone.input = (InputStream) input.clone();
    clone.termInfo = new TermInfo(termInfo);
    if (term != null) clone.growBuffer(term.text.length());

    return clone;
  }

  final void seek(long pointer, int p, Term t, TermInfo ti)
          throws IOException {
    input.seek(pointer);
    position = p;
    term = t;
    prev = null;
    termInfo.set(ti);
    growBuffer(term.text.length());		  // copy term text into buffer
  }

  /** Increments the enumeration to the next element.  True if one exists.*/
  public final boolean next() throws IOException {
    if (position++ >= size - 1) {
      term = null;
      return false;
    }

    prev = term;
    term = readTerm();

    termInfo.docFreq = input.readVInt();	  // read doc freq
    termInfo.freqPointer += input.readVLong();	  // read freq pointer
    termInfo.proxPointer += input.readVLong();	  // read prox pointer

    if (!isIndex) {
      if (termInfo.docFreq > skipInterval) {
        termInfo.skipOffset = input.readVInt();
      }
    }

    if (isIndex)
      indexPointer += input.readVLong();	  // read index pointer

    return true;
  }

  private final Term readTerm() throws IOException {
    int start = input.readVInt();
    int length = input.readVInt();
    int totalLength = start + length;
    if (buffer.length < totalLength)
      growBuffer(totalLength);

    input.readChars(buffer, start, length);
    return new Term(fieldInfos.fieldName(input.readVInt()),
            new String(buffer, 0, totalLength), false);
  }

  private final void growBuffer(int length) {
    buffer = new char[length];
    for (int i = 0; i < term.text.length(); i++)  // copy contents
      buffer[i] = term.text.charAt(i);
  }

  /** Returns the current Term in the enumeration.
   Initially invalid, valid after next() called for the first time.*/
  public final Term term() {
    return term;
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
