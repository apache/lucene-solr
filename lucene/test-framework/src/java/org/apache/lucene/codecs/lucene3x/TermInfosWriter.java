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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.UnicodeUtil;


/** This stores a monotonically increasing set of <Term, TermInfo> pairs in a
  Directory.  A TermInfos can be written once, in order.  */

final class TermInfosWriter implements Closeable {
  /** The file format version, a negative number. */
  public static final int FORMAT = -3;

  // Changed strings to true utf8 with length-in-bytes not
  // length-in-chars
  public static final int FORMAT_VERSION_UTF8_LENGTH_IN_BYTES = -4;

  // NOTE: always change this if you switch to a new format!
  public static final int FORMAT_CURRENT = FORMAT_VERSION_UTF8_LENGTH_IN_BYTES;

  private FieldInfos fieldInfos;
  private IndexOutput output;
  private TermInfo lastTi = new TermInfo();
  private long size;

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

  /** Expert: The fraction of term entries stored in skip tables,
   * used to accelerate skipping.  Larger values result in
   * smaller indexes, greater acceleration, but fewer accelerable cases, while
   * smaller values result in bigger indexes, less acceleration and more
   * accelerable cases. More detailed experiments would be useful here. */
  int skipInterval = 16;
  
  /** Expert: The maximum number of skip levels. Smaller values result in 
   * slightly smaller indexes, but slower skipping in big posting lists.
   */
  int maxSkipLevels = 10;

  private long lastIndexPointer;
  private boolean isIndex;
  private final BytesRef lastTerm = new BytesRef();
  private int lastFieldNumber = -1;

  private TermInfosWriter other;

  TermInfosWriter(Directory directory, String segment, FieldInfos fis,
                  int interval)
       throws IOException {
    initialize(directory, segment, fis, interval, false);
    boolean success = false;
    try {
      other = new TermInfosWriter(directory, segment, fis, interval, true);
      other.other = this;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(output);

        try {
          directory.deleteFile(IndexFileNames.segmentFileName(segment, "",
              (isIndex ? Lucene3xPostingsFormat.TERMS_INDEX_EXTENSION
                  : Lucene3xPostingsFormat.TERMS_EXTENSION)));
        } catch (IOException ignored) {
        }
      }
    }
  }

  private TermInfosWriter(Directory directory, String segment, FieldInfos fis,
                          int interval, boolean isIndex) throws IOException {
    initialize(directory, segment, fis, interval, isIndex);
  }

  private void initialize(Directory directory, String segment, FieldInfos fis,
                          int interval, boolean isi) throws IOException {
    indexInterval = interval;
    fieldInfos = fis;
    isIndex = isi;
    output = directory.createOutput(IndexFileNames.segmentFileName(segment, "",
        (isIndex ? Lucene3xPostingsFormat.TERMS_INDEX_EXTENSION
            : Lucene3xPostingsFormat.TERMS_EXTENSION)), IOContext.DEFAULT);
    boolean success = false;
    try {
      output.writeInt(FORMAT_CURRENT);              // write format
      output.writeLong(0);                          // leave space for size
      output.writeInt(indexInterval);               // write indexInterval
      output.writeInt(skipInterval);                // write skipInterval
      output.writeInt(maxSkipLevels);               // write maxSkipLevels
      assert initUTF16Results();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(output);

        try {
          directory.deleteFile(IndexFileNames.segmentFileName(segment, "",
              (isIndex ? Lucene3xPostingsFormat.TERMS_INDEX_EXTENSION
                  : Lucene3xPostingsFormat.TERMS_EXTENSION)));
        } catch (IOException ignored) {
        }
      }
    }
  }

  // Currently used only by assert statements
  CharsRef utf16Result1;
  CharsRef utf16Result2;
  private final BytesRef scratchBytes = new BytesRef();

  // Currently used only by assert statements
  private boolean initUTF16Results() {
    utf16Result1 = new CharsRef(10);
    utf16Result2 = new CharsRef(10);
    return true;
  }
  
  /** note: -1 is the empty field: "" !!!! */
  static String fieldName(FieldInfos infos, int fieldNumber) {
    FieldInfo fi = infos.fieldInfo(fieldNumber);
    return (fi != null) ? fi.name : "";
  }

  // Currently used only by assert statement
  private int compareToLastTerm(int fieldNumber, BytesRef term) {

    if (lastFieldNumber != fieldNumber) {
      final int cmp = fieldName(fieldInfos, lastFieldNumber).compareTo(fieldName(fieldInfos, fieldNumber));
      // If there is a field named "" (empty string) then we
      // will get 0 on this comparison, yet, it's "OK".  But
      // it's not OK if two different field numbers map to
      // the same name.
      if (cmp != 0 || lastFieldNumber != -1)
        return cmp;
    }

    scratchBytes.copyBytes(term);
    assert lastTerm.offset == 0;
    UnicodeUtil.UTF8toUTF16(lastTerm.bytes, 0, lastTerm.length, utf16Result1);

    assert scratchBytes.offset == 0;
    UnicodeUtil.UTF8toUTF16(scratchBytes.bytes, 0, scratchBytes.length, utf16Result2);

    final int len;
    if (utf16Result1.length < utf16Result2.length)
      len = utf16Result1.length;
    else
      len = utf16Result2.length;

    for(int i=0;i<len;i++) {
      final char ch1 = utf16Result1.chars[i];
      final char ch2 = utf16Result2.chars[i];
      if (ch1 != ch2)
        return ch1-ch2;
    }
    if (utf16Result1.length == 0 && lastFieldNumber == -1) {
      // If there is a field named "" (empty string) with a term text of "" (empty string) then we
      // will get 0 on this comparison, yet, it's "OK". 
      return -1;
    }
    return utf16Result1.length - utf16Result2.length;
  }

  /** Adds a new <<fieldNumber, termBytes>, TermInfo> pair to the set.
    Term must be lexicographically greater than all previous Terms added.
    TermInfo pointers must be positive and greater than all previous.*/
  public void add(int fieldNumber, BytesRef term, TermInfo ti)
    throws IOException {

    assert compareToLastTerm(fieldNumber, term) < 0 ||
      (isIndex && term.length == 0 && lastTerm.length == 0) :
      "Terms are out of order: field=" + fieldName(fieldInfos, fieldNumber) + " (number " + fieldNumber + ")" +
        " lastField=" + fieldName(fieldInfos, lastFieldNumber) + " (number " + lastFieldNumber + ")" +
        " text=" + term.utf8ToString() + " lastText=" + lastTerm.utf8ToString();

    assert ti.freqPointer >= lastTi.freqPointer: "freqPointer out of order (" + ti.freqPointer + " < " + lastTi.freqPointer + ")";
    assert ti.proxPointer >= lastTi.proxPointer: "proxPointer out of order (" + ti.proxPointer + " < " + lastTi.proxPointer + ")";

    if (!isIndex && size % indexInterval == 0) {
      other.add(lastFieldNumber, lastTerm, lastTi);                      // add an index term
    }
    writeTerm(fieldNumber, term);                        // write term

    output.writeVInt(ti.docFreq);                       // write doc freq
    output.writeVLong(ti.freqPointer - lastTi.freqPointer); // write pointers
    output.writeVLong(ti.proxPointer - lastTi.proxPointer);

    if (ti.docFreq >= skipInterval) {
      output.writeVInt(ti.skipOffset);
    }

    if (isIndex) {
      output.writeVLong(other.output.getFilePointer() - lastIndexPointer);
      lastIndexPointer = other.output.getFilePointer(); // write pointer
    }

    lastFieldNumber = fieldNumber;
    lastTi.set(ti);
    size++;
  }

  private void writeTerm(int fieldNumber, BytesRef term)
       throws IOException {

    //System.out.println("  tiw.write field=" + fieldNumber + " term=" + term.utf8ToString());

    // TODO: UTF16toUTF8 could tell us this prefix
    // Compute prefix in common with last term:
    int start = 0;
    final int limit = term.length < lastTerm.length ? term.length : lastTerm.length;
    while(start < limit) {
      if (term.bytes[start+term.offset] != lastTerm.bytes[start+lastTerm.offset])
        break;
      start++;
    }

    final int length = term.length - start;
    output.writeVInt(start);                     // write shared prefix length
    output.writeVInt(length);                  // write delta length
    output.writeBytes(term.bytes, start+term.offset, length);  // write delta bytes
    output.writeVInt(fieldNumber); // write field num
    lastTerm.copyBytes(term);
  }

  /** Called to complete TermInfos creation. */
  public void close() throws IOException {
    try {
      output.seek(4);          // write size after format
      output.writeLong(size);
    } finally {
      try {
        output.close();
      } finally {
        if (!isIndex) {
          other.close();
        }
      }
    }
  }
}
