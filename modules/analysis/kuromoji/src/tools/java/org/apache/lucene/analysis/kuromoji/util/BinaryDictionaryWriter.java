package org.apache.lucene.analysis.kuromoji.util;

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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.RamUsageEstimator;

import org.apache.lucene.analysis.kuromoji.dict.Dictionary;
import org.apache.lucene.analysis.kuromoji.dict.BinaryDictionary;
import org.apache.lucene.analysis.kuromoji.dict.TokenInfoDictionary;

public abstract class BinaryDictionaryWriter {
  protected final Class<? extends BinaryDictionary> implClazz;
  protected ByteBuffer buffer;
  private int targetMapEndOffset = 0, lastWordId = -1, lastSourceId = -1;
  private int[] targetMap = new int[8192];
  private int[] targetMapOffsets = new int[8192];
  private final List<String> posDict = new ArrayList<String>();
  private final Map<String,Integer> posDictLookup = new HashMap<String,Integer>();

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
      sb.append(CSVUtil.quoteEscape(entry[i]));
      if (i < 7) {
        sb.append(',');
      }
    }
    String pos = sb.toString();
    Integer posIndex = posDictLookup.get(pos);
    if (posIndex == null) {
      posIndex = posDict.size();
      posDict.add(pos);
      posDictLookup.put(pos, posIndex);
      assert posDict.size() == posDictLookup.size();
    }
    
    // TODO: what are the parts 9 and 10 that kuromoji does not expose via Token?
    // we need to break all these out (we can structure them inside posdict)
    
    String baseForm = entry[10];
    String reading = entry[11];
    String pronunciation = entry[12];
    
    // extend buffer if necessary
    int left = buffer.remaining();
    // worst case: three short, 4 bytes and features (all as utf-16)
    int worstCase = 6 + 4 + 2*(baseForm.length() + reading.length() + pronunciation.length());
    if (worstCase > left) {
      ByteBuffer newBuffer = ByteBuffer.allocate(ArrayUtil.oversize(buffer.limit() + worstCase - left, 1));
      buffer.flip();
      newBuffer.put(buffer);
      buffer = newBuffer;
    }
    
    buffer.putShort(leftId);
    buffer.putShort(rightId);
    buffer.putShort(wordCost);
    assert posIndex.intValue() < 256;
    buffer.put(posIndex.byteValue());
    
    if (baseForm.equals(entry[0])) {
      buffer.put((byte)0); // base form is the same as surface form
    } else {
      buffer.put((byte)baseForm.length());
      for (int i = 0; i < baseForm.length(); i++) {
        buffer.putChar(baseForm.charAt(i));
      }
    }
    
    if (isKatakana(reading)) {
      buffer.put((byte) (reading.length() << 1 | 1));
      writeKatakana(reading);
    } else {
      buffer.put((byte) (reading.length() << 1));
      for (int i = 0; i < reading.length(); i++) {
        buffer.putChar(reading.charAt(i));
      }
    }
    
    if (pronunciation.equals(reading)) {
      buffer.put((byte)0); // pronunciation is the same as reading
    } else {
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

  protected final String getBaseFileName(String baseDir) throws IOException {
    return baseDir + File.separator + implClazz.getName().replace('.', File.separatorChar);
  }
  
  /**
   * Write dictionary in file
   * Dictionary format is:
   * [Size of dictionary(int)], [entry:{left id(short)}{right id(short)}{word cost(short)}{length of pos info(short)}{pos info(char)}], [entry...], [entry...].....
   * @throws IOException
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
        out.writeString(s);
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
