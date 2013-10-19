package org.apache.lucene.analysis.ko.dic;

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

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;

public class HanjaMapper {
  private HanjaMapper() {}

  private static final int HANJA_START = 0x3400;
  private static final MonotonicBlockPackedReader index;
  private static final char[] data;
  static {
    InputStream datStream = null, idxStream = null;
    try {
      datStream = DictionaryResources.class.getResourceAsStream(DictionaryResources.FILE_HANJA_DAT);
      if (datStream == null)
        throw new FileNotFoundException(DictionaryResources.FILE_HANJA_DAT);
      idxStream = DictionaryResources.class.getResourceAsStream(DictionaryResources.FILE_HANJA_IDX);
      if (idxStream == null)
        throw new FileNotFoundException(DictionaryResources.FILE_HANJA_IDX);
      DataInput dat = new InputStreamDataInput(new BufferedInputStream(datStream));
      DataInput idx = new InputStreamDataInput(new BufferedInputStream(idxStream));
      CodecUtil.checkHeader(dat, DictionaryResources.FILE_HANJA_DAT, DictionaryResources.DATA_VERSION, DictionaryResources.DATA_VERSION);
      CodecUtil.checkHeader(idx, DictionaryResources.FILE_HANJA_IDX, DictionaryResources.DATA_VERSION, DictionaryResources.DATA_VERSION);
      data = new char[dat.readVInt()];
      for (int i = 0; i < data.length; i++) {
        data[i] = (char) dat.readShort();
        assert Character.UnicodeBlock.of(data[i]) == Character.UnicodeBlock.HANGUL_SYLLABLES;
      }
      index = new MonotonicBlockPackedReader(idx, idx.readVInt(), idx.readVInt(), idx.readVInt(), false);
    } catch (IOException ioe) {
      throw new Error("Cannot load resource", ioe);
    } finally {
      IOUtils.closeWhileHandlingException(datStream, idxStream);
    }
  }
  
  /** 
   * Returns array of hangul pronunciations.
   * TODO: expose this in another way */
  public static char[] convertToHangul(char hanja) {
    if (hanja < HANJA_START) {
      return new char[] { hanja };
    } else {
      int idx = hanja - HANJA_START;
      int start = (int) index.get(idx);
      int end = (int) index.get(idx+1);
      if (end - start == 0) {
        return new char[] { hanja };
      } else {
        char result[] = new char[end - start];
        System.arraycopy(data, start, result, 0, end - start);
        return result;
      }
    }
  }
}
