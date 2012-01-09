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
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IOUtils;

public abstract class BinaryDictionary implements Dictionary {
  
  public static final String DICT_FILENAME_SUFFIX = "$buffer.dat";
  public static final String TARGETMAP_FILENAME_SUFFIX = "$targetMap.dat";
  public static final String POSDICT_FILENAME_SUFFIX = "$posDict.dat";
  public static final String INFLDICT_FILENAME_SUFFIX = "$inflDict.dat";
  
  public static final String DICT_HEADER = "kuromoji_dict";
  public static final String TARGETMAP_HEADER = "kuromoji_dict_map";
  public static final String POSDICT_HEADER = "kuromoji_dict_pos";
  public static final String INFLDICT_HEADER = "kuromoji_dict_infl";
  public static final int VERSION = 1;
  
  private final ByteBuffer buffer;
  private final int[] targetMapOffsets, targetMap;
  private final String[] posDict;
  private final String[] inflTypeDict;
  private final String[] inflFormDict;
  
  protected BinaryDictionary() throws IOException {
    InputStream mapIS = null, dictIS = null, posIS = null, inflIS = null;
    IOException priorE = null;
    int[] targetMapOffsets = null, targetMap = null;
    String[] posDict = null;
    String[] inflFormDict = null;
    String[] inflTypeDict = null;
    ByteBuffer buffer = null;
    try {
      mapIS = getResource(TARGETMAP_FILENAME_SUFFIX);
      mapIS = new BufferedInputStream(mapIS);
      DataInput in = new InputStreamDataInput(mapIS);
      CodecUtil.checkHeader(in, TARGETMAP_HEADER, VERSION, VERSION);
      targetMap = new int[in.readVInt()];
      targetMapOffsets = new int[in.readVInt()];
      int accum = 0, sourceId = 0;
      for (int ofs = 0; ofs < targetMap.length; ofs++) {
        final int val = in.readVInt();
        if ((val & 0x01) != 0) {
          targetMapOffsets[sourceId] = ofs;
          sourceId++;
        }
        accum += val >>> 1;
        targetMap[ofs] = accum;
      }
      if (sourceId + 1 != targetMapOffsets.length)
        throw new IOException("targetMap file format broken");
      targetMapOffsets[sourceId] = targetMap.length;
      mapIS.close(); mapIS = null;
      
      posIS = getResource(POSDICT_FILENAME_SUFFIX);
      posIS = new BufferedInputStream(posIS);
      in = new InputStreamDataInput(posIS);
      CodecUtil.checkHeader(in, POSDICT_HEADER, VERSION, VERSION);
      posDict = new String[in.readVInt()];
      for (int j = 0; j < posDict.length; j++) {
        posDict[j] = in.readString();
      }
      posIS.close(); posIS = null;
      
      inflIS = getResource(INFLDICT_FILENAME_SUFFIX);
      inflIS = new BufferedInputStream(inflIS);
      in = new InputStreamDataInput(inflIS);
      CodecUtil.checkHeader(in, INFLDICT_HEADER, VERSION, VERSION);
      int length = in.readVInt();
      inflTypeDict = new String[length];
      inflFormDict = new String[length];
      for (int j = 0; j < length; j++) {
        inflTypeDict[j] = in.readString();
        inflFormDict[j] = in.readString();
      }
      inflIS.close(); inflIS = null;

      dictIS = getResource(DICT_FILENAME_SUFFIX);
      // no buffering here, as we load in one large buffer
      in = new InputStreamDataInput(dictIS);
      CodecUtil.checkHeader(in, DICT_HEADER, VERSION, VERSION);
      final int size = in.readVInt();
      final ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(size);
      final ReadableByteChannel channel = Channels.newChannel(dictIS);
      final int read = channel.read(tmpBuffer);
      if (read != size) {
        throw new EOFException("Cannot read whole dictionary");
      }
      dictIS.close(); dictIS = null;
      buffer = tmpBuffer.asReadOnlyBuffer();
    } catch (IOException ioe) {
      priorE = ioe;
    } finally {
      IOUtils.closeWhileHandlingException(priorE, mapIS, posIS, inflIS, dictIS);
    }
    
    this.targetMap = targetMap;
    this.targetMapOffsets = targetMapOffsets;
    this.posDict = posDict;
    this.inflTypeDict = inflTypeDict;
    this.inflFormDict = inflFormDict;
    this.buffer = buffer;
  }
  
  protected final InputStream getResource(String suffix) throws IOException {
    return getClassResource(getClass(), suffix);
  }
  
  // util, reused by ConnectionCosts and CharacterDefinition
  public static final InputStream getClassResource(Class<?> clazz, String suffix) throws IOException {
    final InputStream is = clazz.getResourceAsStream(clazz.getSimpleName() + suffix);
    if (is == null)
      throw new FileNotFoundException("Not in classpath: " + clazz.getName().replace('.','/') + suffix);
    return is;
  }
  
  public void lookupWordIds(int sourceId, IntsRef ref) {
    ref.ints = targetMap;
    ref.offset = targetMapOffsets[sourceId];
    // targetMapOffsets always has one more entry pointing behind last:
    ref.length = targetMapOffsets[sourceId + 1] - ref.offset;
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
  public String getBaseForm(int wordId) {
    int offset = baseFormOffset(wordId);
    int length = (buffer.get(offset++) & 0xff) >>> 1;
    if (length == 0) {
      return null; // same as surface form
    } else {
      return readString(offset, length, false);
    }
  }
  
  @Override
  public String getReading(int wordId) {
    int offset = readingOffset(wordId);
    int readingData = buffer.get(offset++) & 0xff;
    return readString(offset, readingData >>> 1, (readingData & 1) == 1);
  }
  
  @Override
  public String getPartOfSpeech(int wordId) {
    int posIndex = buffer.get(posOffset(wordId)) & 0xff; // read index into posDict
    return posDict[posIndex >>> 1];
  }
  
  @Override
  public String getPronunciation(int wordId) {
    if (hasPronunciationData(wordId)) {
      int offset = pronunciationOffset(wordId);
      int pronunciationData = buffer.get(offset++) & 0xff;
      return readString(offset, pronunciationData >>> 1, (pronunciationData & 1) == 1);
    } else {
      return getReading(wordId); // same as the reading
    }
  }
  
  @Override
  public String getInflectionType(int wordId) {
    int index = getInflectionIndex(wordId);
    return index < 0 ? null : inflTypeDict[index];
  }

  @Override
  public String getInflectionForm(int wordId) {
    int index = getInflectionIndex(wordId);
    return index < 0 ? null : inflFormDict[index];
  }
  
  private static int posOffset(int wordId) {
    return wordId + 6;
  }
  
  private static int baseFormOffset(int wordId) {
    return wordId + 7;
  }
  
  private int readingOffset(int wordId) {
    int offset = baseFormOffset(wordId);
    int baseFormLength = buffer.get(offset++) & 0xfe; // mask away pronunciation bit
    return offset + baseFormLength;
  }
  
  private int pronunciationOffset(int wordId) {
    int offset = readingOffset(wordId);
    int readingData = buffer.get(offset++) & 0xff;
    final int readingLength;
    if ((readingData & 1) == 0) {
      readingLength = readingData & 0xfe; // UTF-16: mask off kana bit
    } else {
      readingLength = readingData >>> 1;
    }
    return offset + readingLength;
  }
  
  private boolean hasPronunciationData(int wordId) {
    int baseFormData = buffer.get(baseFormOffset(wordId)) & 0xff;
    return (baseFormData & 1) == 0;
  }
  
  private boolean hasInflectionData(int wordId) {
    int posData = buffer.get(posOffset(wordId)) & 0xff;
    return (posData & 1) == 1;
  }
  
  private int getInflectionIndex(int wordId) {
    if (!hasInflectionData(wordId)) {
      return -1; // common case: no inflection data
    }
    
    // skip past reading/pronunciation at the end
    int offset = hasPronunciationData(wordId) ? pronunciationOffset(wordId) : readingOffset(wordId);
    int endData = buffer.get(offset++) & 0xff;
    
    final int endLength;
    if ((endData & 1) == 0) {
      endLength = endData & 0xfe; // UTF-16: mask off kana bit
    } else {
      endLength = endData >>> 1;
    }
    
    offset += endLength;
    
    byte b = buffer.get(offset++);
    int i = b & 0x7F;
    if ((b & 0x80) == 0) return i;
    b = buffer.get(offset++);
    i |= (b & 0x7F) << 7;
    assert ((b & 0x80) == 0);
    return i;
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
}
