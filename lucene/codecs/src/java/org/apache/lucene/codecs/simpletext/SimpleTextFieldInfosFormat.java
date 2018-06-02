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
package org.apache.lucene.codecs.simpletext;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

/**
 * plaintext field infos format
 * <p>
 * <b>FOR RECREATIONAL USE ONLY</b>
 * @lucene.experimental
 */
public class SimpleTextFieldInfosFormat extends FieldInfosFormat {
  
  /** Extension of field infos */
  static final String FIELD_INFOS_EXTENSION = "inf";
  
  static final BytesRef NUMFIELDS       =  new BytesRef("number of fields ");
  static final BytesRef NAME            =  new BytesRef("  name ");
  static final BytesRef NUMBER          =  new BytesRef("  number ");
  static final BytesRef STORETV         =  new BytesRef("  term vectors ");
  static final BytesRef STORETVPOS      =  new BytesRef("  term vector positions ");
  static final BytesRef STORETVOFF      =  new BytesRef("  term vector offsets ");
  static final BytesRef PAYLOADS        =  new BytesRef("  payloads ");
  static final BytesRef NORMS           =  new BytesRef("  norms ");
  static final BytesRef DOCVALUES       =  new BytesRef("  doc values ");
  static final BytesRef DOCVALUES_GEN   =  new BytesRef("  doc values gen ");
  static final BytesRef INDEXOPTIONS    =  new BytesRef("  index options ");
  static final BytesRef NUM_ATTS        =  new BytesRef("  attributes ");
  static final BytesRef ATT_KEY         =  new BytesRef("    key ");
  static final BytesRef ATT_VALUE       =  new BytesRef("    value ");
  static final BytesRef DIM_COUNT       =  new BytesRef("  dimensional count ");
  static final BytesRef DIM_NUM_BYTES   =  new BytesRef("  dimensional num bytes ");
  
  @Override
  public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, FIELD_INFOS_EXTENSION);
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
        assert StringHelper.startsWith(scratch.get(), INDEXOPTIONS);
        String s = readString(INDEXOPTIONS.length, scratch);
        final IndexOptions indexOptions = IndexOptions.valueOf(s);

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

        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), DIM_COUNT);
        int dimensionalCount = Integer.parseInt(readString(DIM_COUNT.length, scratch));

        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), DIM_NUM_BYTES);
        int dimensionalNumBytes = Integer.parseInt(readString(DIM_NUM_BYTES.length, scratch));

        infos[i] = new FieldInfo(name, fieldNumber, storeTermVector, 
                                 omitNorms, storePayloads, indexOptions, docValuesType, dvGen, Collections.unmodifiableMap(atts),
                                 dimensionalCount, dimensionalNumBytes);
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
    return DocValuesType.valueOf(dvType);
  }
  
  private String readString(int offset, BytesRefBuilder scratch) {
    return new String(scratch.bytes(), offset, scratch.length()-offset, StandardCharsets.UTF_8);
  }

  @Override
  public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, FIELD_INFOS_EXTENSION);
    IndexOutput out = directory.createOutput(fileName, context);
    BytesRefBuilder scratch = new BytesRefBuilder();
    boolean success = false;
    try {
      SimpleTextUtil.write(out, NUMFIELDS);
      SimpleTextUtil.write(out, Integer.toString(infos.size()), scratch);
      SimpleTextUtil.writeNewline(out);
      
      for (FieldInfo fi : infos) {
        SimpleTextUtil.write(out, NAME);
        SimpleTextUtil.write(out, fi.name, scratch);
        SimpleTextUtil.writeNewline(out);
        
        SimpleTextUtil.write(out, NUMBER);
        SimpleTextUtil.write(out, Integer.toString(fi.number), scratch);
        SimpleTextUtil.writeNewline(out);
        
        SimpleTextUtil.write(out, INDEXOPTIONS);
        IndexOptions indexOptions = fi.getIndexOptions();
        assert indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !fi.hasPayloads();
        SimpleTextUtil.write(out, indexOptions.toString(), scratch);
        SimpleTextUtil.writeNewline(out);
        
        SimpleTextUtil.write(out, STORETV);
        SimpleTextUtil.write(out, Boolean.toString(fi.hasVectors()), scratch);
        SimpleTextUtil.writeNewline(out);
        
        SimpleTextUtil.write(out, PAYLOADS);
        SimpleTextUtil.write(out, Boolean.toString(fi.hasPayloads()), scratch);
        SimpleTextUtil.writeNewline(out);
               
        SimpleTextUtil.write(out, NORMS);
        SimpleTextUtil.write(out, Boolean.toString(!fi.omitsNorms()), scratch);
        SimpleTextUtil.writeNewline(out);

        SimpleTextUtil.write(out, DOCVALUES);
        SimpleTextUtil.write(out, getDocValuesType(fi.getDocValuesType()), scratch);
        SimpleTextUtil.writeNewline(out);
        
        SimpleTextUtil.write(out, DOCVALUES_GEN);
        SimpleTextUtil.write(out, Long.toString(fi.getDocValuesGen()), scratch);
        SimpleTextUtil.writeNewline(out);
               
        Map<String,String> atts = fi.attributes();
        int numAtts = atts == null ? 0 : atts.size();
        SimpleTextUtil.write(out, NUM_ATTS);
        SimpleTextUtil.write(out, Integer.toString(numAtts), scratch);
        SimpleTextUtil.writeNewline(out);
      
        if (numAtts > 0) {
          for (Map.Entry<String,String> entry : atts.entrySet()) {
            SimpleTextUtil.write(out, ATT_KEY);
            SimpleTextUtil.write(out, entry.getKey(), scratch);
            SimpleTextUtil.writeNewline(out);
          
            SimpleTextUtil.write(out, ATT_VALUE);
            SimpleTextUtil.write(out, entry.getValue(), scratch);
            SimpleTextUtil.writeNewline(out);
          }
        }

        SimpleTextUtil.write(out, DIM_COUNT);
        SimpleTextUtil.write(out, Integer.toString(fi.getPointDimensionCount()), scratch);
        SimpleTextUtil.writeNewline(out);
        
        SimpleTextUtil.write(out, DIM_NUM_BYTES);
        SimpleTextUtil.write(out, Integer.toString(fi.getPointNumBytes()), scratch);
        SimpleTextUtil.writeNewline(out);
      }
      SimpleTextUtil.writeChecksum(out, scratch);
      success = true;
    } finally {
      if (success) {
        out.close();
      } else {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }
  
  private static String getDocValuesType(DocValuesType type) {
    return type.toString();
  }
}
