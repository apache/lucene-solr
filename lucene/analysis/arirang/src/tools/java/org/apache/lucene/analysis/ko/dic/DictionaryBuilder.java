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
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

public class DictionaryBuilder {
  public static void main(String args[]) throws Exception {
    String FILES_AS_IS[] = { 
      DictionaryResources.FILE_COMPOUNDS,
      DictionaryResources.FILE_DICTIONARY,
      DictionaryResources.FILE_EOMI,
      DictionaryResources.FILE_EXTENSION,
      DictionaryResources.FILE_JOSA,
      DictionaryResources.FILE_PREFIX,
      DictionaryResources.FILE_SUFFIX,
      DictionaryResources.FILE_SYLLABLE_FEATURE,
      DictionaryResources.FILE_UNCOMPOUNDS
    };
    
    File inputDir = new File(args[0]);
    File outputDir = new File(args[1]);
    for (String file : FILES_AS_IS) {
      File in = new File(inputDir, file);
      File out = new File(outputDir, file);
      copyAsIs(in, out);
    }
    buildHanjaMap(inputDir, outputDir);
  }
  
  static void copyAsIs(File in, File out) throws Exception {
    InputStream r = new BufferedInputStream(new FileInputStream(in));
    OutputStream w = new BufferedOutputStream(new FileOutputStream(out));
    int c;
    while ((c = r.read()) != -1) {
      w.write(c);
    }
    r.close();
    w.close();
  }
  
  static void buildHanjaMap(File inputDir, File outputDir) throws Exception {
    final int HANJA_START = 0x3400;
    final int IDX_SIZE = 0x10000 - HANJA_START;
    OutputStream idxStream = new BufferedOutputStream(new FileOutputStream(new File(outputDir, DictionaryResources.FILE_HANJA_IDX)));
    DataOutput idx = new OutputStreamDataOutput(idxStream);
    CodecUtil.writeHeader(idx, DictionaryResources.FILE_HANJA_IDX, DictionaryResources.DATA_VERSION);
    idx.writeVInt(PackedInts.VERSION_CURRENT);
    idx.writeVInt(1024);
    idx.writeVInt(IDX_SIZE+1); // CJK: first half of unicode, compat: at the end. but monotonic's blocking works here (?)
    MonotonicBlockPackedWriter idxArray = new MonotonicBlockPackedWriter(idx, 1024);
    
    OutputStream datStream = new BufferedOutputStream(new FileOutputStream(new File(outputDir, DictionaryResources.FILE_HANJA_DAT)));
    DataOutput dat = new OutputStreamDataOutput(datStream);
    CodecUtil.writeHeader(dat, DictionaryResources.FILE_HANJA_DAT, DictionaryResources.DATA_VERSION);
    char datArray[] = new char[256];
    File input = new File(inputDir, "mapHanja.dic");
    BufferedReader reader = new BufferedReader(IOUtils.getDecodingReader(input, IOUtils.CHARSET_UTF_8));
    int currentInput = -1;
    int currentOutput = 0;
    String line = null;
    while ((line = reader.readLine()) != null) {
      if (line.startsWith("!")) {
        continue;
      }
      int cp = line.charAt(0) - HANJA_START;
      while (currentInput < cp) {
        idxArray.add(currentOutput);
        currentInput++;
      }
      String mappings = line.substring(2);
      for (int i = 0; i < mappings.length(); i++) {
        if (currentOutput == datArray.length) {
          datArray = ArrayUtil.grow(datArray);
        }
        datArray[currentOutput] = mappings.charAt(i);
        currentOutput++;
      }
      currentInput = cp;
    }
    while (currentInput < IDX_SIZE) {
      idxArray.add(currentOutput);
      currentInput++;
    }
    idxArray.finish();
    dat.writeVInt(currentOutput);
    for (int i = 0; i < currentOutput; i++) {
      dat.writeShort((short) datArray[i]); 
    }
    idxStream.close();
    datStream.close();
  }
}
