package org.apache.lucene.index.codecs.simpletext;

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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.codecs.FieldInfosReader;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.index.codecs.simpletext.SimpleTextFieldInfosWriter.*;

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
      assert scratch.startsWith(NUMFIELDS);
      final int size = Integer.parseInt(readString(NUMFIELDS.length, scratch));
      FieldInfo infos[] = new FieldInfo[size];

      for (int i = 0; i < size; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert scratch.startsWith(NAME);
        String name = readString(NAME.length, scratch);
        
        SimpleTextUtil.readLine(input, scratch);
        assert scratch.startsWith(NUMBER);
        int fieldNumber = Integer.parseInt(readString(NUMBER.length, scratch));

        SimpleTextUtil.readLine(input, scratch);
        assert scratch.startsWith(ISINDEXED);
        boolean isIndexed = Boolean.parseBoolean(readString(ISINDEXED.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert scratch.startsWith(STORETV);
        boolean storeTermVector = Boolean.parseBoolean(readString(STORETV.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert scratch.startsWith(STORETVPOS);
        boolean storePositionsWithTermVector = Boolean.parseBoolean(readString(STORETVPOS.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert scratch.startsWith(STORETVOFF);
        boolean storeOffsetWithTermVector = Boolean.parseBoolean(readString(STORETVOFF.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert scratch.startsWith(PAYLOADS);
        boolean storePayloads = Boolean.parseBoolean(readString(PAYLOADS.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert scratch.startsWith(NORMS);
        boolean omitNorms = !Boolean.parseBoolean(readString(NORMS.length, scratch));

        SimpleTextUtil.readLine(input, scratch);
        assert scratch.startsWith(DOCVALUES);
        String dvType = readString(DOCVALUES.length, scratch);
        final ValueType docValuesType;
        
        if ("false".equals(dvType)) {
          docValuesType = null;
        } else {
          docValuesType = ValueType.valueOf(dvType);
        }
        
        SimpleTextUtil.readLine(input, scratch);
        assert scratch.startsWith(INDEXOPTIONS);
        IndexOptions indexOptions = IndexOptions.valueOf(readString(INDEXOPTIONS.length, scratch));

        hasVectors |= storeTermVector;
        hasProx |= isIndexed && indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        hasFreq |= isIndexed && indexOptions != IndexOptions.DOCS_ONLY;
        
        infos[i] = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, 
          storePositionsWithTermVector, storeOffsetWithTermVector, 
          omitNorms, storePayloads, indexOptions, docValuesType);
      }

      if (input.getFilePointer() != input.length()) {
        throw new CorruptIndexException("did not read all bytes from file \"" + fileName + "\": read " + input.getFilePointer() + " vs size " + input.length() + " (resource: " + input + ")");
      }
      
      return new FieldInfos(infos, hasFreq, hasProx, hasVectors);
    } finally {
      input.close();
    }
  }
  
  private String readString(int offset, BytesRef scratch) {
    return new String(scratch.bytes, scratch.offset+offset, scratch.length-offset, IOUtils.CHARSET_UTF_8);
  }
  
  public static void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(info.name, "", FIELD_INFOS_EXTENSION));
  }
}
