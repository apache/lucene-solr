package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import org.apache.lucene.store.InputStream;

final class SegmentTermEnum extends TermEnum implements Cloneable {
  private InputStream input;
  private FieldInfos fieldInfos;
  int size;
  int position = -1;

  private Term term = new Term("", "");
  private TermInfo termInfo = new TermInfo();

  boolean isIndex = false;
  long indexPointer = 0;
  Term prev;

  private char[] buffer = {};

  SegmentTermEnum(InputStream i, FieldInfos fis, boolean isi)
       throws IOException {
    input = i;
    fieldInfos = fis; 
    size = input.readInt();
    isIndex = isi;
  }
  
  protected Object clone() {
    SegmentTermEnum clone = null;
    try {
      clone = (SegmentTermEnum)super.clone();
    } catch (CloneNotSupportedException e) {}

    clone.input = (InputStream)input.clone();
    clone.termInfo = new TermInfo(termInfo);
    clone.growBuffer(term.text.length());

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
    if (position++ >= size-1) {
      term = null;
      return false;
    }

    prev = term;
    term = readTerm();

    termInfo.docFreq = input.readVInt();	  // read doc freq
    termInfo.freqPointer += input.readVLong();	  // read freq pointer
    termInfo.proxPointer += input.readVLong();	  // read prox pointer
    
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
