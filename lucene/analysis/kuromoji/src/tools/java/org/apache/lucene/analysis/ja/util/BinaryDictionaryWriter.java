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
package org.apache.lucene.analysis.ja.util;


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;

import org.apache.lucene.analysis.ja.dict.BinaryDictionary;

public abstract class BinaryDictionaryWriter {
  protected final Class<? extends BinaryDictionary> implClazz;
  protected ByteBuffer buffer;
  private int targetMapEndOffset = 0, lastWordId = -1, lastSourceId = -1;
  private int[] targetMap = new int[8192];
  private int[] targetMapOffsets = new int[8192];
  private final ArrayList<String> posDict = new ArrayList<>();

  public BinaryDictionaryWriter(Class<? extends BinaryDictionary> implClazz, int size) {
    this.implClazz = implClazz;
    buffer = ByteBuffer.allocate(size);
  }
  
  /**
   * put the entry in map
   * @return current position of buffer, which will be wordId of next entry
   */
  public int put(String[] entry) {
    short leftId = Short.parseShort(entry[1]);
    short rightId = Short.parseShort(entry[2]);
    short wordCost = Short.parseShort(entry[3]);
    
    StringBuilder sb = new StringBuilder();
    
    // build up the POS string
    for (int i = 4; i < 8; i++) {
      String part = entry[i];
      assert part.length() > 0;
      if (!"*".equals(part)) {
        if (sb.length() > 0) {
          sb.append('-');
        }
        sb.append(part);
      }
    }
    
    String posData = sb.toString();
    
    sb.setLength(0);
    sb.append(CSVUtil.quoteEscape(posData));
    sb.append(',');
    if (!"*".equals(entry[8])) {
      sb.append(CSVUtil.quoteEscape(entry[8]));
    }
    sb.append(',');
    if (!"*".equals(entry[9])) {
      sb.append(CSVUtil.quoteEscape(entry[9]));
    }
    String fullPOSData = sb.toString();
    
    String baseForm = entry[10];
    String reading = entry[11];
    String pronunciation = entry[12];
    
    // extend buffer if necessary
    int left = buffer.remaining();
    // worst case: two short, 3 bytes, and features (all as utf-16)
    int worstCase = 4 + 3 + 2*(baseForm.length() + reading.length() + pronunciation.length());
    if (worstCase > left) {
      ByteBuffer newBuffer = ByteBuffer.allocate(ArrayUtil.oversize(buffer.limit() + worstCase - left, 1));
      buffer.flip();
      newBuffer.put(buffer);
      buffer = newBuffer;
    }

    int flags = 0;
    if (!("*".equals(baseForm) || baseForm.equals(entry[0]))) {
      flags |= BinaryDictionary.HAS_BASEFORM;
    }
    if (!reading.equals(toKatakana(entry[0]))) {
      flags |= BinaryDictionary.HAS_READING;
    }
    if (!pronunciation.equals(reading)) {
      flags |= BinaryDictionary.HAS_PRONUNCIATION;
    }

    assert leftId == rightId;
    assert leftId < 4096; // there are still unused bits
    // add pos mapping
    int toFill = 1+leftId - posDict.size();
    for (int i = 0; i < toFill; i++) {
      posDict.add(null);
    }
    
    String existing = posDict.get(leftId);
    assert existing == null || existing.equals(fullPOSData);
    posDict.set(leftId, fullPOSData);
    
    buffer.putShort((short)(leftId << 3 | flags));
    buffer.putShort(wordCost);

    if ((flags & BinaryDictionary.HAS_BASEFORM) != 0) {
      assert baseForm.length() < 16;
      int shared = sharedPrefix(entry[0], baseForm);
      int suffix = baseForm.length() - shared;
      buffer.put((byte) (shared << 4 | suffix));
      for (int i = shared; i < baseForm.length(); i++) {
        buffer.putChar(baseForm.charAt(i));
      }
    }
    
    if ((flags & BinaryDictionary.HAS_READING) != 0) {
      if (isKatakana(reading)) {
        buffer.put((byte) (reading.length() << 1 | 1));
        writeKatakana(reading);
      } else {
        buffer.put((byte) (reading.length() << 1));
        for (int i = 0; i < reading.length(); i++) {
          buffer.putChar(reading.charAt(i));
        }
      }
    }
    
    if ((flags & BinaryDictionary.HAS_PRONUNCIATION) != 0) {
      // we can save 150KB here, but it makes the reader a little complicated.
      // int shared = sharedPrefix(reading, pronunciation);
      // buffer.put((byte) shared);
      // pronunciation = pronunciation.substring(shared);
      if (isKatakana(pronunciation)) {
        buffer.put((byte) (pronunciation.length() << 1 | 1));
        writeKatakana(pronunciation);
      } else {
        buffer.put((byte) (pronunciation.length() << 1));
        for (int i = 0; i < pronunciation.length(); i++) {
          buffer.putChar(pronunciation.charAt(i));
        }
      }
    }
    
    return buffer.position();
  }
  
  private boolean isKatakana(String s) {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (ch < 0x30A0 || ch > 0x30FF) {
        return false;
      }
    }
    return true;
  }
  
  private void writeKatakana(String s) {
    for (int i = 0; i < s.length(); i++) {
      buffer.put((byte) (s.charAt(i) - 0x30A0));
    }
  }
  
  private String toKatakana(String s) {
    char text[] = new char[s.length()];
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (ch > 0x3040 && ch < 0x3097) {
        text[i] = (char)(ch + 0x60);
      } else {
        text[i] = ch;
      }
    }
    return new String(text);
  }
  
  public static int sharedPrefix(String left, String right) {
    int len = left.length() < right.length() ? left.length() : right.length();
    for (int i = 0; i < len; i++)
      if (left.charAt(i) != right.charAt(i))
        return i;
    return len;
  }
  
  public void addMapping(int sourceId, int wordId) {
    assert wordId > lastWordId : "words out of order: " + wordId + " vs lastID: " + lastWordId;
    
    if (sourceId > lastSourceId) {
      assert sourceId > lastSourceId : "source ids out of order: lastSourceId=" + lastSourceId + " vs sourceId=" + sourceId;
      targetMapOffsets = ArrayUtil.grow(targetMapOffsets, sourceId + 1);
      for (int i = lastSourceId + 1; i <= sourceId; i++) {
        targetMapOffsets[i] = targetMapEndOffset;
      }
    } else {
      assert sourceId == lastSourceId;
    }

    targetMap = ArrayUtil.grow(targetMap, targetMapEndOffset + 1);
    targetMap[targetMapEndOffset] = wordId;
    targetMapEndOffset++;

    lastSourceId = sourceId;
    lastWordId = wordId;
  }

  protected final String getBaseFileName(String baseDir) {
    return baseDir + File.separator + implClazz.getName().replace('.', File.separatorChar);
  }
  
  /**
   * Write dictionary in file
   * Dictionary format is:
   * [Size of dictionary(int)], [entry:{left id(short)}{right id(short)}{word cost(short)}{length of pos info(short)}{pos info(char)}], [entry...], [entry...].....
   * @throws IOException if an I/O error occurs writing the dictionary files
   */
  public void write(String baseDir) throws IOException {
    final String baseName = getBaseFileName(baseDir);
    writeDictionary(baseName + BinaryDictionary.DICT_FILENAME_SUFFIX);
    writeTargetMap(baseName + BinaryDictionary.TARGETMAP_FILENAME_SUFFIX);
    writePosDict(baseName + BinaryDictionary.POSDICT_FILENAME_SUFFIX);
  }
  
  // TODO: maybe this int[] should instead be the output to the FST...
  protected void writeTargetMap(String filename) throws IOException {
    new File(filename).getParentFile().mkdirs();
    OutputStream os = new FileOutputStream(filename);
    try {
      os = new BufferedOutputStream(os);
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, BinaryDictionary.TARGETMAP_HEADER, BinaryDictionary.VERSION);
      
      final int numSourceIds = lastSourceId + 1;
      out.writeVInt(targetMapEndOffset); // <-- size of main array
      out.writeVInt(numSourceIds + 1); // <-- size of offset array (+ 1 more entry)
      int prev = 0, sourceId = 0;
      for (int ofs = 0; ofs < targetMapEndOffset; ofs++) {
        final int val = targetMap[ofs], delta = val - prev;
        assert delta >= 0;
        if (ofs == targetMapOffsets[sourceId]) {
          out.writeVInt((delta << 1) | 0x01);
          sourceId++;
        } else {
          out.writeVInt((delta << 1));
        }
        prev += delta;
      }
      assert sourceId == numSourceIds : "sourceId:"+sourceId+" != numSourceIds:"+numSourceIds;
    } finally {
      os.close();
    }
  }
  
  protected void writePosDict(String filename) throws IOException {
    new File(filename).getParentFile().mkdirs();
    OutputStream os = new FileOutputStream(filename);
    try {
      os = new BufferedOutputStream(os);
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, BinaryDictionary.POSDICT_HEADER, BinaryDictionary.VERSION);
      out.writeVInt(posDict.size());
      for (String s : posDict) {
        if (s == null) {
          out.writeByte((byte)0);
          out.writeByte((byte)0);
          out.writeByte((byte)0);
        } else {
          String data[] = CSVUtil.parse(s);
          assert data.length == 3 : "malformed pos/inflection: " + s;
          out.writeString(data[0]);
          out.writeString(data[1]);
          out.writeString(data[2]);
        }
      }
    } finally {
      os.close();
    }
  }
  
  protected void writeDictionary(String filename) throws IOException {
    new File(filename).getParentFile().mkdirs();
    final FileOutputStream os = new FileOutputStream(filename);
    try {
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, BinaryDictionary.DICT_HEADER, BinaryDictionary.VERSION);
      out.writeVInt(buffer.position());
      final WritableByteChannel channel = Channels.newChannel(os);
      // Write Buffer
      buffer.flip();  // set position to 0, set limit to current position
      channel.write(buffer);
      assert buffer.remaining() == 0L;
    } finally {
      os.close();
    }
  }
}
