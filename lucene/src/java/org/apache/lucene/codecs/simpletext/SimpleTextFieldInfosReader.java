package org.apache.lucene.codecs.simpletext;

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

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
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
  public FieldInfos read(Directory directory, String segmentName, IOContext iocontext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, "", FIELD_INFOS_EXTENSION);
    IndexInput input = directory.openInput(fileName, iocontext);
    BytesRef scratch = new BytesRef();
    
    boolean hasVectors = false;
    boolean hasFreq = false;
    boolean hasProx = false;
    
    try {
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, NUMFIELDS);
      final int size = Integer.parseInt(readString(NUMFIELDS.length, scratch));
      FieldInfo infos[] = new FieldInfo[size];

      for (int i = 0; i < size; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, NAME);
        String name = readString(NAME.length, scratch);
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, NUMBER);
        int fieldNumber = Integer.parseInt(readString(NUMBER.length, scratch));

        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, ISINDEXED);
        boolean isIndexed = Boolean.parseBoolean(readString(ISINDEXED.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, STORETV);
        boolean storeTermVector = Boolean.parseBoolean(readString(STORETV.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, PAYLOADS);
        boolean storePayloads = Boolean.parseBoolean(readString(PAYLOADS.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, NORMS);
        boolean omitNorms = !Boolean.parseBoolean(readString(NORMS.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, NORMS_TYPE);
        String nrmType = readString(NORMS_TYPE.length, scratch);
        final DocValues.Type normsType = docValuesType(nrmType);
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, DOCVALUES);
        String dvType = readString(DOCVALUES.length, scratch);
        final DocValues.Type docValuesType = docValuesType(dvType);
        
        
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, INDEXOPTIONS);
        IndexOptions indexOptions = IndexOptions.valueOf(readString(INDEXOPTIONS.length, scratch));

        hasVectors |= storeTermVector;
        hasProx |= isIndexed && indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        hasFreq |= isIndexed && indexOptions != IndexOptions.DOCS_ONLY;
        
        infos[i] = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, 
          omitNorms, storePayloads, indexOptions, docValuesType, normsType);
      }

      if (input.getFilePointer() != input.length()) {
        throw new CorruptIndexException("did not read all bytes from file \"" + fileName + "\": read " + input.getFilePointer() + " vs size " + input.length() + " (resource: " + input + ")");
      }
      
      return new FieldInfos(infos, hasFreq, hasProx, hasVectors);
    } finally {
      input.close();
    }
  }

  public DocValues.Type docValuesType(String dvType) {
    if ("false".equals(dvType)) {
      return null;
    } else {
      return DocValues.Type.valueOf(dvType);
    }
  }
  
  private String readString(int offset, BytesRef scratch) {
    return new String(scratch.bytes, scratch.offset+offset, scratch.length-offset, IOUtils.CHARSET_UTF_8);
  }
  
  public static void files(SegmentInfo info, Set<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(info.name, "", FIELD_INFOS_EXTENSION));
  }
}
