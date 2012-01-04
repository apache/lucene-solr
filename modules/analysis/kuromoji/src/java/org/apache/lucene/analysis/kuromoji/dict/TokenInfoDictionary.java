package org.apache.lucene.analysis.kuromoji.dict;

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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.CodecUtil;

import org.apache.lucene.analysis.kuromoji.util.CSVUtil;

public class TokenInfoDictionary implements Dictionary {
  
  public static final String FILENAME = "tid.dat";
  public static final String TARGETMAP_FILENAME = "tid_map.dat";
  
  public static final String TARGETMAP_HEADER = "kuromoji_dict_map";
  public static final String DICT_HEADER = "kuromoji_dict";
  public static final int VERSION = 1;
  
  protected ByteBuffer buffer;
  
  protected int[][] targetMap;
  
  public TokenInfoDictionary() {
  }
  
  public TokenInfoDictionary(int size) {
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
      sb.append(entry[i]).append(INTERNAL_SEPARATOR);
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
  
  public int[] lookupWordIds(int sourceId) {
    return targetMap[sourceId];
  }
  
  @Override	
  public int getLeftId(int wordId) {
    return buffer.getShort(wordId);
  }
  
  @Override
  public int getRightId(int wordId) {
    return buffer.getShort(wordId + 2);	// Skip left id
  }
  
  @Override
  public int getWordCost(int wordId) {
    return buffer.getShort(wordId + 4);	// Skip left id and right id
  }
  
  @Override
  public String[] getAllFeaturesArray(int wordId) {
    int size = buffer.getShort(wordId + 6) / 2; // Read length of feature String. Skip 6 bytes, see data structure.
    char[] targetArr = new char[size];
    int offset = wordId + 6 + 2; // offset is position where features string starts
    for(int i = 0; i < size; i++){
      targetArr[i] = buffer.getChar(offset + i * 2);
    }
    String allFeatures = new String(targetArr);
    return allFeatures.split(INTERNAL_SEPARATOR);
  }
  
  @Override
  public String getFeature(int wordId, int... fields) {
    String[] allFeatures = getAllFeaturesArray(wordId);
    StringBuilder sb = new StringBuilder();
    
    if(fields.length == 0){ // All features
      for(String feature : allFeatures) {
        sb.append(CSVUtil.quoteEscape(feature)).append(",");
      }
    } else if(fields.length == 1) { // One feature doesn't need to escape value
      sb.append(allFeatures[fields[0]]).append(",");			
    } else {
      for(int field : fields){
        sb.append(CSVUtil.quoteEscape(allFeatures[field])).append(",");
      }
    }
    
    return sb.deleteCharAt(sb.length() - 1).toString();
  }
  
  @Override
  public String getReading(int wordId) {
    return getFeature(wordId, 7);
  }
  
  @Override
  public String getAllFeatures(int wordId) {
    return getFeature(wordId);
  }
  
  @Override
  public String getPartOfSpeech(int wordId) {
    return getFeature(wordId, 0, 1, 2, 3);
  }
  
  @Override
  public String getBaseForm(int wordId) {
    String form = getFeature(wordId, 6);
    return "*".equals(form) ? null : form;
  }

  /**
   * Write dictionary in file
   * Dictionary format is:
   * [Size of dictionary(int)], [entry:{left id(short)}{right id(short)}{word cost(short)}{length of pos info(short)}{pos info(char)}], [entry...], [entry...].....
   * @throws IOException
   */
  public void write(String directoryname) throws IOException {
    writeDictionary(directoryname + File.separator + FILENAME);
    writeTargetMap(directoryname + File.separator + TARGETMAP_FILENAME);
  }
  
  protected void writeTargetMap(String filename) throws IOException {
    OutputStream os = new FileOutputStream(filename);
    try {
      os = new BufferedOutputStream(os);
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, TARGETMAP_HEADER, VERSION);
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
      CodecUtil.writeHeader(out, DICT_HEADER, VERSION);
      out.writeVInt(buffer.position());
      final WritableByteChannel channel = Channels.newChannel(os);
      // Write Buffer
      buffer.flip();  // set position to 0, set limit to current position
      channel.write(buffer);
    } finally {
      os.close();
    }
  }
  
  /**
   * Read dictionary into directly allocated buffer.
   * @return TokenInfoDictionary instance
   * @throws IOException
   * @throws ClassNotFoundException 
   */
  public static TokenInfoDictionary getInstance() throws IOException, ClassNotFoundException {
    TokenInfoDictionary dictionary = new TokenInfoDictionary();
    dictionary.loadDictionary(TokenInfoDictionary.class.getResourceAsStream(FILENAME));
    dictionary.loadTargetMap(TokenInfoDictionary.class.getResourceAsStream(TARGETMAP_FILENAME));
    return dictionary;
  }
  
  protected void loadTargetMap(InputStream is) throws IOException, ClassNotFoundException {
    is = new BufferedInputStream(is);
    try {
      final DataInput in = new InputStreamDataInput(is);
      CodecUtil.checkHeader(in, TARGETMAP_HEADER, VERSION, VERSION);
      targetMap = new int[in.readVInt()][];
      for (int j = 0; j < targetMap.length;) {
        final int len = in.readVInt();
        if (len == 0) {
          // decode RLE: number of nulls
          j += in.readVInt();
        } else {
          final int[] a = new int[len];
          for (int i = 0; i < len; i++) {
            a[i] = in.readVInt();
          }
          targetMap[j] = a;
          j++;
        }
      }
    } finally {
      is.close();
    }
  }
  
  protected void loadDictionary(InputStream is) throws IOException {
    try {
      final DataInput in = new InputStreamDataInput(is);
      CodecUtil.checkHeader(in, DICT_HEADER, VERSION, VERSION);
      final int size = in.readVInt();
      final ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(size);
      final ReadableByteChannel channel = Channels.newChannel(is);
      final int read = channel.read(tmpBuffer);
      if (read != size) {
        throw new EOFException("Cannot read whole dictionary");
      }
      buffer = tmpBuffer.asReadOnlyBuffer();
    } finally {
      is.close();
    }
  }
  
}
