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
import org.apache.lucene.analysis.kuromoji.trie.DoubleArrayTrie;

public class TokenInfoDictionaryWriter {
  protected ByteBuffer buffer;
  private int targetMapSize = 0;
  private int[][] targetMap = new int[8192][];
  private int[] targetMapComponentSizes = new int[8192];
  private final List<String> posDict = new ArrayList<String>();
  private final Map<String,Integer> posDictLookup = new HashMap<String,Integer>();

  public TokenInfoDictionaryWriter(int size) {
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
    if(targetMap.length <= sourceId) {
      final int newSize = ArrayUtil.oversize(sourceId + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      int[][] newArray = new int[newSize][];
      System.arraycopy(targetMap, 0, newArray, 0, targetMap.length);
      targetMap = newArray;
      int[] newSizeArray = new int[newSize];
      System.arraycopy(targetMapComponentSizes, 0, newSizeArray, 0, targetMapComponentSizes.length);
      targetMapComponentSizes = newSizeArray;
    }
    
    // Prepare array -- extend the length of array
    int[] current = targetMap[sourceId];
    if (current == null) {
      assert targetMapComponentSizes[sourceId] == 0;
      current = new int[1];
    } else {
      current = ArrayUtil.grow(current);
    }
    targetMap[sourceId] = current;
    
    int[] targets = targetMap[sourceId];
    targets[targetMapComponentSizes[sourceId]] = wordId;
    targetMapComponentSizes[sourceId]++;
    targetMapSize = Math.max(targetMapSize, sourceId + 1);
  }

  /**
   * Write dictionary in file
   * Dictionary format is:
   * [Size of dictionary(int)], [entry:{left id(short)}{right id(short)}{word cost(short)}{length of pos info(short)}{pos info(char)}], [entry...], [entry...].....
   * @throws IOException
   */
  public void write(String baseDir) throws IOException {
    final String baseName = baseDir + File.separator + TokenInfoDictionary.class.getName().replace('.', File.separatorChar);
    writeDictionary(baseName + BinaryDictionary.DICT_FILENAME_SUFFIX);
    writeTargetMap(baseName + BinaryDictionary.TARGETMAP_FILENAME_SUFFIX);
    writePosDict(baseName + BinaryDictionary.POSDICT_FILENAME_SUFFIX);
  }
  
  protected void writeTargetMap(String filename) throws IOException {
    new File(filename).getParentFile().mkdirs();
    OutputStream os = new FileOutputStream(filename);
    try {
      os = new BufferedOutputStream(os);
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, BinaryDictionary.TARGETMAP_HEADER, BinaryDictionary.VERSION);
      out.writeVInt(targetMapSize);
      int nulls = 0;
      for (int j = 0; j < targetMapSize; j++) {
        final int size = targetMapComponentSizes[j];
        if (size == 0) {
          // run-length encoding for all nulls:
          if (nulls == 0) {
            out.writeVInt(0);
          }
          nulls++;
        } else {
          if (nulls > 0) {
            out.writeVInt(nulls);
            nulls = 0;
          }
          final int[] a = targetMap[j];
          assert size > 0 && size <= a.length;
          out.writeVInt(size);
          for (int i = 0; i < size; i++) {
            out.writeVInt(a[i]);
          }
        }
      }
      // write the pending RLE count:
      if (nulls > 0) {
        out.writeVInt(nulls);
      }
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
    System.out.println("Info: wrote " + posDict.size() + " unique POS entries");
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
  
  public static void writeDoubleArrayTrie(String baseDir, DoubleArrayTrie trie) throws IOException  {
    String filename = baseDir + File.separator + TokenInfoDictionary.class.getName().replace('.', File.separatorChar) + TokenInfoDictionary.TRIE_FILENAME_SUFFIX;
    new File(filename).getParentFile().mkdirs();
    
    final FileOutputStream os = new FileOutputStream(filename);
    try {
      trie.write(os);
    } finally {
      os.close();
    }
  }
  
}
