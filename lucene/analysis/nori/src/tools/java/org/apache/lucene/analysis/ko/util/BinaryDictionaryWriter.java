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
package org.apache.lucene.analysis.ko.util;

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

import org.apache.lucene.analysis.ko.POS;
import org.apache.lucene.analysis.ko.dict.Dictionary;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;

import org.apache.lucene.analysis.ko.dict.BinaryDictionary;

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
   *
   * mecab-ko-dic features
   *
   * 0   - surface
   * 1   - left cost
   * 2   - right cost
   * 3   - word cost
   * 4   - part of speech0+part of speech1+...
   * 5   - semantic class
   * 6   - T if the last character of the surface form has a coda, F otherwise
   * 7   - reading
   * 8   - POS type (*, Compound, Inflect, Preanalysis)
   * 9   - left POS
   * 10  - right POS
   * 11  - expression
   *
   * @return current position of buffer, which will be wordId of next entry
   */
  public int put(String[] entry) {
    short leftId = Short.parseShort(entry[1]);
    short rightId = Short.parseShort(entry[2]);
    short wordCost = Short.parseShort(entry[3]);

    final POS.Type posType = POS.resolveType(entry[8]);
    final POS.Tag leftPOS;
    final POS.Tag rightPOS;
    if (posType == POS.Type.MORPHEME || posType == POS.Type.COMPOUND || entry[9].equals("*")) {
      leftPOS = POS.resolveTag(entry[4]);
      assert (entry[9].equals("*") && entry[10].equals("*"));
      rightPOS = leftPOS;
    } else {
      leftPOS = POS.resolveTag(entry[9]);
      rightPOS = POS.resolveTag(entry[10]);
    }
    final String reading = entry[7].equals("*") ? "" : entry[0].equals(entry[7]) ? "" : entry[7];
    final String expression = entry[11].equals("*") ? "" : entry[11];

    // extend buffer if necessary
    int left = buffer.remaining();
    // worst case, 3 short + 4 bytes and features (all as utf-16)
    int worstCase = 9 + 2*(expression.length() + reading.length());
    if (worstCase > left) {
      ByteBuffer newBuffer = ByteBuffer.allocate(ArrayUtil.oversize(buffer.limit() + worstCase - left, 1));
      buffer.flip();
      newBuffer.put(buffer);
      buffer = newBuffer;
    }

    // add pos mapping
    int toFill = 1+leftId - posDict.size();
    for (int i = 0; i < toFill; i++) {
      posDict.add(null);
    }
    String fullPOSData = leftPOS.name() + "," + entry[5];
    String existing = posDict.get(leftId);
    assert existing == null || existing.equals(fullPOSData);
    posDict.set(leftId, fullPOSData);

    final Dictionary.Morpheme[] morphemes;
    // true if the POS and decompounds of the token are all the same.
    boolean hasSinglePOS = (leftPOS == rightPOS);
    if (posType != POS.Type.MORPHEME && expression.length() > 0) {
      String[] exprTokens = expression.split("\\+");
      morphemes = new Dictionary.Morpheme[exprTokens.length];
      for (int i = 0; i < exprTokens.length; i++) {
        String[] tokenSplit = exprTokens[i].split("\\/");
        assert tokenSplit.length == 3;
        POS.Tag exprTag = POS.resolveTag(tokenSplit[1]);
        morphemes[i] = new Dictionary.Morpheme(exprTag, tokenSplit[0]);
        if (leftPOS != exprTag) {
          hasSinglePOS = false;
        }
      }
    } else {
      morphemes = new Dictionary.Morpheme[0];
    }

    int flags = 0;
    if (hasSinglePOS) {
      flags |= BinaryDictionary.HAS_SINGLE_POS;
    }
    if (posType == POS.Type.MORPHEME && reading.length() > 0) {
      flags |= BinaryDictionary.HAS_READING;
    }

    assert leftId < 8192; // there are still unused bits
    assert posType.ordinal() < 4;
    buffer.putShort((short)(leftId << 2 | posType.ordinal()));
    buffer.putShort((short) (rightId << 2 | flags));
    buffer.putShort(wordCost);

    if (posType == POS.Type.MORPHEME) {
      assert leftPOS == rightPOS;
      if (reading.length() > 0) {
        writeString(reading);
      }
    } else {
      if (hasSinglePOS == false) {
        buffer.put((byte) rightPOS.ordinal());
      }
      buffer.put((byte) morphemes.length);
      int compoundOffset = 0;
      for (int i = 0; i < morphemes.length; i++) {
        if (hasSinglePOS == false) {
          buffer.put((byte) morphemes[i].posTag.ordinal());
        }
        if (posType != POS.Type.INFLECT) {
          buffer.put((byte) morphemes[i].surfaceForm.length());
          compoundOffset += morphemes[i].surfaceForm.length();
        } else {
          writeString(morphemes[i].surfaceForm);
        }
        assert compoundOffset <= entry[0].length() : Arrays.toString(entry);
      }
    }
    return buffer.position();
  }

  private void writeString(String s) {
    buffer.put((byte) s.length());
    for (int i = 0; i < s.length(); i++) {
      buffer.putChar(s.charAt(i));
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
  
  protected final String getBaseFileName(String baseDir) {
    return baseDir + File.separator + implClazz.getName().replace('.', File.separatorChar);
  }

  /**
   * Write dictionary in file
   * @throws IOException if an I/O error occurs writing the dictionary files
   */
  public void write(String baseDir) throws IOException {
    final String baseName = getBaseFileName(baseDir);
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
          out.writeByte((byte) POS.Tag.UNKNOWN.ordinal());
        } else {
          String data[] = CSVUtil.parse(s);
          assert data.length == 2 : "malformed pos/semanticClass: " + s;
          out.writeByte((byte) POS.Tag.valueOf(data[0]).ordinal());
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
