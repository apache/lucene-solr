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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.UnicodeUtil;

/** This stores a monotonically increasing set of <Term, TermInfo> pairs in a
  Directory.  A TermInfos can be written once, in order.  */

final class TermInfosWriter {
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

  /** Expert: The fraction of {@link TermDocs} entries stored in skip tables,
   * used to accellerate {@link TermDocs#skipTo(int)}.  Larger values result in
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
  private byte[] lastTermBytes = new byte[10];
  private int lastTermBytesLength = 0;
  private int lastFieldNumber = -1;

  private TermInfosWriter other;
  private UnicodeUtil.UTF8Result utf8Result = new UnicodeUtil.UTF8Result();

  TermInfosWriter(Directory directory, String segment, FieldInfos fis,
                  int interval)
       throws IOException {
    initialize(directory, segment, fis, interval, false);
    other = new TermInfosWriter(directory, segment, fis, interval, true);
    other.other = this;
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
    output = directory.createOutput(segment + (isIndex ? ".tii" : ".tis"));
    output.writeInt(FORMAT_CURRENT);              // write format
    output.writeLong(0);                          // leave space for size
    output.writeInt(indexInterval);               // write indexInterval
    output.writeInt(skipInterval);                // write skipInterval
    output.writeInt(maxSkipLevels);               // write maxSkipLevels
    assert initUTF16Results();
  }

  void add(Term term, TermInfo ti) throws IOException {
    UnicodeUtil.UTF16toUTF8(term.text, 0, term.text.length(), utf8Result);
    add(fieldInfos.fieldNumber(term.field), utf8Result.result, utf8Result.length, ti);
  }

  // Currently used only by assert statements
  UnicodeUtil.UTF16Result utf16Result1;
  UnicodeUtil.UTF16Result utf16Result2;

  // Currently used only by assert statements
  private boolean initUTF16Results() {
    utf16Result1 = new UnicodeUtil.UTF16Result();
    utf16Result2 = new UnicodeUtil.UTF16Result();
    return true;
  }

  // Currently used only by assert statement
  private int compareToLastTerm(int fieldNumber, byte[] termBytes, int termBytesLength) {

    if (lastFieldNumber != fieldNumber) {
      final int cmp = fieldInfos.fieldName(lastFieldNumber).compareTo(fieldInfos.fieldName(fieldNumber));
      // If there is a field named "" (empty string) then we
      // will get 0 on this comparison, yet, it's "OK".  But
      // it's not OK if two different field numbers map to
      // the same name.
      if (cmp != 0 || lastFieldNumber != -1)
        return cmp;
    }

    UnicodeUtil.UTF8toUTF16(lastTermBytes, 0, lastTermBytesLength, utf16Result1);
    UnicodeUtil.UTF8toUTF16(termBytes, 0, termBytesLength, utf16Result2);
    final int len;
    if (utf16Result1.length < utf16Result2.length)
      len = utf16Result1.length;
    else
      len = utf16Result2.length;

    for(int i=0;i<len;i++) {
      final char ch1 = utf16Result1.result[i];
      final char ch2 = utf16Result2.result[i];
      if (ch1 != ch2)
        return ch1-ch2;
    }
    return utf16Result1.length - utf16Result2.length;
  }

  /** Adds a new <<fieldNumber, termBytes>, TermInfo> pair to the set.
    Term must be lexicographically greater than all previous Terms added.
    TermInfo pointers must be positive and greater than all previous.*/
  void add(int fieldNumber, byte[] termBytes, int termBytesLength, TermInfo ti)
    throws IOException {

    assert compareToLastTerm(fieldNumber, termBytes, termBytesLength) < 0 ||
      (isIndex && termBytesLength == 0 && lastTermBytesLength == 0) :
      "Terms are out of order: field=" + fieldInfos.fieldName(fieldNumber) + " (number " + fieldNumber + ")" +
        " lastField=" + fieldInfos.fieldName(lastFieldNumber) + " (number " + lastFieldNumber + ")" +
        " text=" + new String(termBytes, 0, termBytesLength, "UTF-8") + " lastText=" + new String(lastTermBytes, 0, lastTermBytesLength, "UTF-8");

    assert ti.freqPointer >= lastTi.freqPointer: "freqPointer out of order (" + ti.freqPointer + " < " + lastTi.freqPointer + ")";
    assert ti.proxPointer >= lastTi.proxPointer: "proxPointer out of order (" + ti.proxPointer + " < " + lastTi.proxPointer + ")";

    if (!isIndex && size % indexInterval == 0)
      other.add(lastFieldNumber, lastTermBytes, lastTermBytesLength, lastTi);                      // add an index term

    writeTerm(fieldNumber, termBytes, termBytesLength);                        // write term

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

  private void writeTerm(int fieldNumber, byte[] termBytes, int termBytesLength)
       throws IOException {

    // TODO: UTF16toUTF8 could tell us this prefix
    // Compute prefix in common with last term:
    int start = 0;
    final int limit = termBytesLength < lastTermBytesLength ? termBytesLength : lastTermBytesLength;
    while(start < limit) {
      if (termBytes[start] != lastTermBytes[start])
        break;
      start++;
    }

    final int length = termBytesLength - start;
    output.writeVInt(start);                     // write shared prefix length
    output.writeVInt(length);                  // write delta length
    output.writeBytes(termBytes, start, length);  // write delta bytes
    output.writeVInt(fieldNumber); // write field num
    if (lastTermBytes.length < termBytesLength) {
      byte[] newArray = new byte[(int) (termBytesLength*1.5)];
      System.arraycopy(lastTermBytes, 0, newArray, 0, start);
      lastTermBytes = newArray;
    }
    System.arraycopy(termBytes, start, lastTermBytes, start, length);
    lastTermBytesLength = termBytesLength;
  }

  /** Called to complete TermInfos creation. */
  void close() throws IOException {
    output.seek(4);          // write size after format
    output.writeLong(size);
    output.close();

    if (!isIndex)
      other.close();
  }

}
