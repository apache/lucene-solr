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
import java.io.EOFException;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;

import org.apache.lucene.analysis.kuromoji.util.CSVUtil;

public abstract class BinaryDictionary implements Dictionary {
  
  public static final String DICT_FILENAME_SUFFIX = "$buffer.dat";
  public static final String TARGETMAP_FILENAME_SUFFIX = "$targetMap.dat";
  
  public static final String TARGETMAP_HEADER = "kuromoji_dict_map";
  public static final String DICT_HEADER = "kuromoji_dict";
  public static final int VERSION = 1;
  
  private final ByteBuffer buffer;
  private final int[][] targetMap;
  
  protected BinaryDictionary() throws IOException {
    InputStream mapIS = null, dictIS = null;
    IOException priorE = null;
    int[][] targetMap = null;
    ByteBuffer buffer = null;
    try {
      mapIS = getClass().getResourceAsStream(getClass().getSimpleName() + TARGETMAP_FILENAME_SUFFIX);
      if (mapIS == null)
        throw new FileNotFoundException("Not in classpath: " + getClass().getName().replace('.','/') + TARGETMAP_FILENAME_SUFFIX);
      mapIS = new BufferedInputStream(mapIS);
      DataInput in = new InputStreamDataInput(mapIS);
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

      dictIS = getClass().getResourceAsStream(getClass().getSimpleName() + DICT_FILENAME_SUFFIX);
      if (dictIS == null)
        throw new FileNotFoundException("Not in classpath: " + getClass().getName().replace('.','/') + DICT_FILENAME_SUFFIX);
      in = new InputStreamDataInput(dictIS);
      CodecUtil.checkHeader(in, DICT_HEADER, VERSION, VERSION);
      final int size = in.readVInt();
      final ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(size);
      final ReadableByteChannel channel = Channels.newChannel(dictIS);
      final int read = channel.read(tmpBuffer);
      if (read != size) {
        throw new EOFException("Cannot read whole dictionary");
      }
      buffer = tmpBuffer.asReadOnlyBuffer();
    } catch (IOException ioe) {
      priorE = ioe;
    } finally {
      IOUtils.closeWhileHandlingException(priorE, mapIS, dictIS);
    }
    
    this.targetMap = targetMap;
    this.buffer = buffer;
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
  
}
