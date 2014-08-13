package org.apache.lucene.codecs.simpletext;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.codecs.simpletext.SimpleTextFieldInfosWriter.*;

/**
 * reads plaintext field infos files
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextFieldInfosReader extends FieldInfosReader {

  @Override
  public FieldInfos read(Directory directory, String segmentName, String segmentSuffix, IOContext iocontext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, segmentSuffix, FIELD_INFOS_EXTENSION);
    ChecksumIndexInput input = directory.openChecksumInput(fileName, iocontext);
    BytesRefBuilder scratch = new BytesRefBuilder();
    
    boolean success = false;
    try {
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), NUMFIELDS);
      final int size = Integer.parseInt(readString(NUMFIELDS.length, scratch));
      FieldInfo infos[] = new FieldInfo[size];

      for (int i = 0; i < size; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), NAME);
        String name = readString(NAME.length, scratch);
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), NUMBER);
        int fieldNumber = Integer.parseInt(readString(NUMBER.length, scratch));

        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), ISINDEXED);
        boolean isIndexed = Boolean.parseBoolean(readString(ISINDEXED.length, scratch));
        
        final IndexOptions indexOptions;
        if (isIndexed) {
          SimpleTextUtil.readLine(input, scratch);
          assert StringHelper.startsWith(scratch.get(), INDEXOPTIONS);
          indexOptions = IndexOptions.valueOf(readString(INDEXOPTIONS.length, scratch));          
        } else {
          indexOptions = null;
        }
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), STORETV);
        boolean storeTermVector = Boolean.parseBoolean(readString(STORETV.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), PAYLOADS);
        boolean storePayloads = Boolean.parseBoolean(readString(PAYLOADS.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), NORMS);
        boolean omitNorms = !Boolean.parseBoolean(readString(NORMS.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), NORMS_TYPE);
        String nrmType = readString(NORMS_TYPE.length, scratch);
        final DocValuesType normsType = docValuesType(nrmType);
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), DOCVALUES);
        String dvType = readString(DOCVALUES.length, scratch);
        final DocValuesType docValuesType = docValuesType(dvType);
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), DOCVALUES_GEN);
        final long dvGen = Long.parseLong(readString(DOCVALUES_GEN.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), NUM_ATTS);
        int numAtts = Integer.parseInt(readString(NUM_ATTS.length, scratch));
        Map<String,String> atts = new HashMap<>();

        for (int j = 0; j < numAtts; j++) {
          SimpleTextUtil.readLine(input, scratch);
          assert StringHelper.startsWith(scratch.get(), ATT_KEY);
          String key = readString(ATT_KEY.length, scratch);
        
          SimpleTextUtil.readLine(input, scratch);
          assert StringHelper.startsWith(scratch.get(), ATT_VALUE);
          String value = readString(ATT_VALUE.length, scratch);
          atts.put(key, value);
        }

        infos[i] = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, 
          omitNorms, storePayloads, indexOptions, docValuesType, normsType, dvGen, Collections.unmodifiableMap(atts));
      }

      SimpleTextUtil.checkFooter(input);
      
      FieldInfos fieldInfos = new FieldInfos(infos);
      success = true;
      return fieldInfos;
    } finally {
      if (success) {
        input.close();
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }

  public DocValuesType docValuesType(String dvType) {
    if ("false".equals(dvType)) {
      return null;
    } else {
      return DocValuesType.valueOf(dvType);
    }
  }
  
  private String readString(int offset, BytesRefBuilder scratch) {
    return new String(scratch.bytes(), offset, scratch.length()-offset, StandardCharsets.UTF_8);
  }
}
