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
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.store.Directory;

/** This stores a monotonically increasing set of <Term, TermInfo> pairs in a
  Directory.  A TermInfos can be written once, in order.  */

final class TermInfosWriter {
  /** The file format version, a negative number. */
  public static final int FORMAT = -1;

  private FieldInfos fieldInfos;
  private OutputStream output;
  private Term lastTerm = new Term("", "");
  private TermInfo lastTi = new TermInfo();
  private int size = 0;

  // TODO: the default values for these two parameters should be settable from
  // IndexWriter.  However, once that's done, folks will start setting them to
  // ridiculous values and complaining that things don't work well, as with
  // mergeFactor.  So, let's wait until a number of folks find that alternate
  // values work better.  Note that both of these values are stored in the
  // segment, so that it's safe to change these w/o rebuilding all indexes.

  /** Expert: The fraction of terms in the "dictionary" which should be stored
   * in RAM.  Smaller values use more memory, but make searching slightly
   * faster, while larger values use less memory and make searching slightly
   * slower.  Searching is typically not dominated by dictionary lookup, so
   * tweaking this is rarely useful.*/
  int indexInterval = 128;

  /** Expert: The fraction of {@link TermDocs} entries stored in skip tables,
   * used to accellerate {@link TermDocs#skipTo(int)}.  Larger values result in
   * smaller indexes, greater acceleration, but fewer accelerable cases, while
   * smaller values result in bigger indexes, less acceleration and more
   * accelerable cases. More detailed experiments would be useful here. */
  int skipInterval = 16;

  private long lastIndexPointer = 0;
  private boolean isIndex = false;

  private TermInfosWriter other = null;

  TermInfosWriter(Directory directory, String segment, FieldInfos fis)
       throws IOException {
    initialize(directory, segment, fis, false);
    other = new TermInfosWriter(directory, segment, fis, true);
    other.other = this;
  }

  private TermInfosWriter(Directory directory, String segment, FieldInfos fis,
			  boolean isIndex) throws IOException {
    initialize(directory, segment, fis, isIndex);
  }

  private void initialize(Directory directory, String segment, FieldInfos fis,
		     boolean isi) throws IOException {
    fieldInfos = fis;
    isIndex = isi;
    output = directory.createFile(segment + (isIndex ? ".tii" : ".tis"));
    output.writeInt(FORMAT);                      // write format
    output.writeLong(0);                          // leave space for size
    if (!isIndex) {
      output.writeInt(indexInterval);             // write indexInterval
      output.writeInt(skipInterval);              // write skipInterval
    }
  }

  /** Adds a new <Term, TermInfo> pair to the set.
    Term must be lexicographically greater than all previous Terms added.
    TermInfo pointers must be positive and greater than all previous.*/
  final void add(Term term, TermInfo ti)
       throws IOException {
    if (!isIndex && term.compareTo(lastTerm) <= 0)
      throw new IOException("term out of order");
    if (ti.freqPointer < lastTi.freqPointer)
      throw new IOException("freqPointer out of order");
    if (ti.proxPointer < lastTi.proxPointer)
      throw new IOException("proxPointer out of order");

    if (!isIndex && size % indexInterval == 0)
      other.add(lastTerm, lastTi);		  // add an index term

    writeTerm(term);				  // write term
    output.writeVInt(ti.docFreq);		  // write doc freq
    output.writeVLong(ti.freqPointer - lastTi.freqPointer); // write pointers
    output.writeVLong(ti.proxPointer - lastTi.proxPointer);

    if (!isIndex) {
      if (ti.docFreq > skipInterval) {
        output.writeVInt(ti.skipOffset);
      }
    }

    if (isIndex) {
      output.writeVLong(other.output.getFilePointer() - lastIndexPointer);
      lastIndexPointer = other.output.getFilePointer(); // write pointer
    }

    lastTi.set(ti);
    size++;
  }

  private final void writeTerm(Term term)
       throws IOException {
    int start = stringDifference(lastTerm.text, term.text);
    int length = term.text.length() - start;

    output.writeVInt(start);			  // write shared prefix length
    output.writeVInt(length);			  // write delta length
    output.writeChars(term.text, start, length);  // write delta chars

    output.writeVInt(fieldInfos.fieldNumber(term.field)); // write field num

    lastTerm = term;
  }

  private static final int stringDifference(String s1, String s2) {
    int len1 = s1.length();
    int len2 = s2.length();
    int len = len1 < len2 ? len1 : len2;
    for (int i = 0; i < len; i++)
      if (s1.charAt(i) != s2.charAt(i))
	return i;
    return len;
  }

  /** Called to complete TermInfos creation. */
  final void close() throws IOException {
    output.seek(4);				  // write size after format
    output.writeLong(size);
    output.close();

    if (!isIndex)
      other.close();
  }
}
