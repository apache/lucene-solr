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

public abstract class BinaryDictionary implements Dictionary {
  
  public static final String DICT_FILENAME_SUFFIX = "$buffer.dat";
  public static final String TARGETMAP_FILENAME_SUFFIX = "$targetMap.dat";
  public static final String POSDICT_FILENAME_SUFFIX = "$posDict.dat";
  
  public static final String DICT_HEADER = "kuromoji_dict";
  public static final String TARGETMAP_HEADER = "kuromoji_dict_map";
  public static final String POSDICT_HEADER = "kuromoji_dict_pos";
  public static final int VERSION = 1;
  
  private final ByteBuffer buffer;
  private final int[][] targetMap;
  private final String[] posDict;
  
  protected BinaryDictionary() throws IOException {
    InputStream mapIS = null, dictIS = null, posIS = null;
    IOException priorE = null;
    int[][] targetMap = null;
    String[] posDict = null;
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
      
      posIS = getClass().getResourceAsStream(getClass().getSimpleName() + POSDICT_FILENAME_SUFFIX);
      if (posIS == null)
        throw new FileNotFoundException("Not in classpath: " + getClass().getName().replace('.','/') + POSDICT_FILENAME_SUFFIX);
      posIS = new BufferedInputStream(posIS);
      in = new InputStreamDataInput(posIS);
      CodecUtil.checkHeader(in, POSDICT_HEADER, VERSION, VERSION);
      posDict = new String[in.readVInt()];
      for (int j = 0; j < posDict.length; j++) {
        posDict[j] = in.readString();
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
      IOUtils.closeWhileHandlingException(priorE, mapIS, posIS, dictIS);
    }
    
    this.targetMap = targetMap;
    this.posDict = posDict;
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

  private String readString(int offset, int length, boolean kana) {
    char text[] = new char[length];
    if (kana) {
      for (int i = 0; i < length; i++) {
        text[i] = (char) (0x30A0 + (buffer.get(offset + i) & 0xff));
      }
    } else {
      for (int i = 0; i < length; i++) {
        text[i] = buffer.getChar(offset + (i << 1));
      }
    }
    return new String(text);
  }
  
  @Override
  public String getReading(int wordId) {
    int offset = wordId + 7;
    int baseFormLength = buffer.get(offset++) & 0xff;
    offset += baseFormLength << 1;
    int readingData = buffer.get(offset++) & 0xff;
    return readString(offset, readingData >>> 1, (readingData & 1) == 1);
  }
  
  @Override
  public String getPronunciation(int wordId) {
    int offset = wordId + 7;
    int baseFormLength = buffer.get(offset++) & 0xff;
    offset += baseFormLength << 1;
    int readingData = buffer.get(offset++) & 0xff;
    int readingLength = readingData >>> 1;
    int readingOffset = offset;
    if ((readingData & 1) == 0) {
      offset += readingLength << 1;
    } else {
      offset += readingLength;
    }
    int pronunciationData = buffer.get(offset++) & 0xff;
    if (pronunciationData == 0) {
      return readString(readingOffset, readingLength, (readingData & 1) == 1); 
    } else {
      return readString(offset, pronunciationData >>> 1, (pronunciationData & 1) == 1);
    }
  }
  
  @Override
  public String getPartOfSpeech(int wordId) {
    int posIndex = buffer.get(wordId + 6) & 0xff; // read index into posDict
    return posDict[posIndex];
  }
  
  @Override
  public String getBaseForm(int wordId) {
    int offset = wordId + 7;
    int length = buffer.get(offset++) & 0xff;
    if (length == 0) {
      return null; // same as surface form
    } else {
      return readString(offset, length, false);
    }
  }
}
