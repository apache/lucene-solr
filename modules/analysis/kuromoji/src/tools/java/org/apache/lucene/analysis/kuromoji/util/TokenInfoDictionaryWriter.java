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

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.CodecUtil;

import org.apache.lucene.analysis.kuromoji.dict.Dictionary;
import org.apache.lucene.analysis.kuromoji.dict.TokenInfoDictionary;

public class TokenInfoDictionaryWriter {
  protected ByteBuffer buffer;
  protected int[][] targetMap = new int[1][];
  
  public TokenInfoDictionaryWriter(int size) {
    targetMap = new int[1][];
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
    for (int i = 4; i < entry.length; i++){
      sb.append(entry[i]).append(Dictionary.INTERNAL_SEPARATOR);
    }
    String features = sb.deleteCharAt(sb.length() - 1).toString();
    int featuresSize = features.length()* 2;
    
    // extend buffer if necessary
    int left = buffer.limit() - buffer.position();
    if (8 + featuresSize > left) { // four short and features
      ByteBuffer newBuffer = ByteBuffer.allocate(buffer.limit() * 2);
      buffer.flip();
      newBuffer.put(buffer);
      buffer = newBuffer;
    }
    
    buffer.putShort(leftId);
    buffer.putShort(rightId);
    buffer.putShort(wordCost);
    buffer.putShort((short)featuresSize);
    for (char c : features.toCharArray()){
      buffer.putChar(c);
    }
    
    return buffer.position();
  }
  
  public void addMapping(int sourceId, int wordId) {
    if(targetMap.length <= sourceId) {
      int[][] newArray = new int[sourceId + 1][];
      System.arraycopy(targetMap, 0, newArray, 0, targetMap.length);
      targetMap = newArray;
    }
    
    // Prepare array -- extend the length of array by one
    int[] current = targetMap[sourceId];
    if (current == null) {
      current = new int[1];
    } else {
      int[] newArray = new int[current.length + 1];
      System.arraycopy(current, 0, newArray, 0, current.length);
      current = newArray;
    }
    targetMap[sourceId] = current;
    
    int[] targets = targetMap[sourceId];
    targets[targets.length - 1] = wordId;
  }

  /**
   * Write dictionary in file
   * Dictionary format is:
   * [Size of dictionary(int)], [entry:{left id(short)}{right id(short)}{word cost(short)}{length of pos info(short)}{pos info(char)}], [entry...], [entry...].....
   * @throws IOException
   */
  public void write(String baseDir) throws IOException {
    writeDictionary(baseDir + File.separator + TokenInfoDictionary.class.getName().replace('.', File.separatorChar) + TokenInfoDictionary.DICT_FILENAME_SUFFIX);
    writeTargetMap(baseDir + File.separator + TokenInfoDictionary.class.getName().replace('.', File.separatorChar) + TokenInfoDictionary.TARGETMAP_FILENAME_SUFFIX);
  }
  
  protected void writeTargetMap(String filename) throws IOException {
    OutputStream os = new FileOutputStream(filename);
    try {
      os = new BufferedOutputStream(os);
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, TokenInfoDictionary.TARGETMAP_HEADER, TokenInfoDictionary.VERSION);
      out.writeVInt(targetMap.length);
      int nulls = 0;
      for (int[] a : targetMap) {
        if (a == null) {
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
          assert a.length > 0;
          out.writeVInt(a.length);
          for (int i = 0; i < a.length; i++) {
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
  
  protected void writeDictionary(String filename) throws IOException {
    final FileOutputStream os = new FileOutputStream(filename);
    try {
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, TokenInfoDictionary.DICT_HEADER, TokenInfoDictionary.VERSION);
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
