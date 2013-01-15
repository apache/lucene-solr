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
import java.util.Map;

import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * writes plaintext field infos files
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextFieldInfosWriter extends FieldInfosWriter {
  
  /** Extension of field infos */
  static final String FIELD_INFOS_EXTENSION = "inf";
  
  static final BytesRef NUMFIELDS       =  new BytesRef("number of fields ");
  static final BytesRef NAME            =  new BytesRef("  name ");
  static final BytesRef NUMBER          =  new BytesRef("  number ");
  static final BytesRef ISINDEXED       =  new BytesRef("  indexed ");
  static final BytesRef STORETV         =  new BytesRef("  term vectors ");
  static final BytesRef STORETVPOS      =  new BytesRef("  term vector positions ");
  static final BytesRef STORETVOFF      =  new BytesRef("  term vector offsets ");
  static final BytesRef PAYLOADS        =  new BytesRef("  payloads ");
  static final BytesRef NORMS           =  new BytesRef("  norms ");
  static final BytesRef NORMS_TYPE      =  new BytesRef("  norms type ");
  static final BytesRef DOCVALUES       =  new BytesRef("  doc values ");
  static final BytesRef INDEXOPTIONS    =  new BytesRef("  index options ");
  static final BytesRef NUM_ATTS        =  new BytesRef("  attributes ");
  final static BytesRef ATT_KEY         =  new BytesRef("    key ");
  final static BytesRef ATT_VALUE       =  new BytesRef("    value ");
  
  @Override
  public void write(Directory directory, String segmentName, FieldInfos infos, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, "", FIELD_INFOS_EXTENSION);
    IndexOutput out = directory.createOutput(fileName, context);
    BytesRef scratch = new BytesRef();
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
        
        SimpleTextUtil.write(out, ISINDEXED);
        SimpleTextUtil.write(out, Boolean.toString(fi.isIndexed()), scratch);
        SimpleTextUtil.writeNewline(out);
        
        if (fi.isIndexed()) {
          assert fi.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !fi.hasPayloads();
          SimpleTextUtil.write(out, INDEXOPTIONS);
          SimpleTextUtil.write(out, fi.getIndexOptions().toString(), scratch);
          SimpleTextUtil.writeNewline(out);
        }
        
        SimpleTextUtil.write(out, STORETV);
        SimpleTextUtil.write(out, Boolean.toString(fi.hasVectors()), scratch);
        SimpleTextUtil.writeNewline(out);
        
        SimpleTextUtil.write(out, PAYLOADS);
        SimpleTextUtil.write(out, Boolean.toString(fi.hasPayloads()), scratch);
        SimpleTextUtil.writeNewline(out);
               
        SimpleTextUtil.write(out, NORMS);
        SimpleTextUtil.write(out, Boolean.toString(!fi.omitsNorms()), scratch);
        SimpleTextUtil.writeNewline(out);
        
        SimpleTextUtil.write(out, NORMS_TYPE);
        SimpleTextUtil.write(out, getDocValuesType(fi.getNormType()), scratch);
        SimpleTextUtil.writeNewline(out);
        
        SimpleTextUtil.write(out, DOCVALUES);
        SimpleTextUtil.write(out, getDocValuesType(fi.getDocValuesType()), scratch);
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
      }
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
    return type == null ? "false" : type.toString();
  }
}
