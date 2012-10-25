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
import java.util.Set;

import org.apache.lucene.codecs.SegmentInfoWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * writes plaintext segments files
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextSegmentInfoWriter extends SegmentInfoWriter {

  final static BytesRef SI_VERSION          = new BytesRef("    version ");
  final static BytesRef SI_DOCCOUNT         = new BytesRef("    number of documents ");
  final static BytesRef SI_USECOMPOUND      = new BytesRef("    uses compound file ");
  final static BytesRef SI_NUM_DIAG         = new BytesRef("    diagnostics ");
  final static BytesRef SI_DIAG_KEY         = new BytesRef("      key ");
  final static BytesRef SI_DIAG_VALUE       = new BytesRef("      value ");
  final static BytesRef SI_NUM_ATTS         = new BytesRef("    attributes ");
  final static BytesRef SI_ATT_KEY          = new BytesRef("      key ");
  final static BytesRef SI_ATT_VALUE        = new BytesRef("      value ");
  final static BytesRef SI_NUM_FILES        = new BytesRef("    files ");
  final static BytesRef SI_FILE             = new BytesRef("      file ");
  
  @Override
  public void write(Directory dir, SegmentInfo si, FieldInfos fis, IOContext ioContext) throws IOException {

    String segFileName = IndexFileNames.segmentFileName(si.name, "", SimpleTextSegmentInfoFormat.SI_EXTENSION);
    si.addFile(segFileName);

    boolean success = false;
    IndexOutput output = dir.createOutput(segFileName,  ioContext);

    try {
      BytesRef scratch = new BytesRef();
    
      SimpleTextUtil.write(output, SI_VERSION);
      SimpleTextUtil.write(output, si.getVersion(), scratch);
      SimpleTextUtil.writeNewline(output);
    
      SimpleTextUtil.write(output, SI_DOCCOUNT);
      SimpleTextUtil.write(output, Integer.toString(si.getDocCount()), scratch);
      SimpleTextUtil.writeNewline(output);
    
      SimpleTextUtil.write(output, SI_USECOMPOUND);
      SimpleTextUtil.write(output, Boolean.toString(si.getUseCompoundFile()), scratch);
      SimpleTextUtil.writeNewline(output);
    
      Map<String,String> diagnostics = si.getDiagnostics();
      int numDiagnostics = diagnostics == null ? 0 : diagnostics.size();
      SimpleTextUtil.write(output, SI_NUM_DIAG);
      SimpleTextUtil.write(output, Integer.toString(numDiagnostics), scratch);
      SimpleTextUtil.writeNewline(output);
    
      if (numDiagnostics > 0) {
        for (Map.Entry<String,String> diagEntry : diagnostics.entrySet()) {
          SimpleTextUtil.write(output, SI_DIAG_KEY);
          SimpleTextUtil.write(output, diagEntry.getKey(), scratch);
          SimpleTextUtil.writeNewline(output);
        
          SimpleTextUtil.write(output, SI_DIAG_VALUE);
          SimpleTextUtil.write(output, diagEntry.getValue(), scratch);
          SimpleTextUtil.writeNewline(output);
        }
      }
      
      Map<String,String> atts = si.attributes();
      int numAtts = atts == null ? 0 : atts.size();
      SimpleTextUtil.write(output, SI_NUM_ATTS);
      SimpleTextUtil.write(output, Integer.toString(numAtts), scratch);
      SimpleTextUtil.writeNewline(output);
    
      if (numAtts > 0) {
        for (Map.Entry<String,String> entry : atts.entrySet()) {
          SimpleTextUtil.write(output, SI_ATT_KEY);
          SimpleTextUtil.write(output, entry.getKey(), scratch);
          SimpleTextUtil.writeNewline(output);
        
          SimpleTextUtil.write(output, SI_ATT_VALUE);
          SimpleTextUtil.write(output, entry.getValue(), scratch);
          SimpleTextUtil.writeNewline(output);
        }
      }

      Set<String> files = si.files();
      int numFiles = files == null ? 0 : files.size();
      SimpleTextUtil.write(output, SI_NUM_FILES);
      SimpleTextUtil.write(output, Integer.toString(numFiles), scratch);
      SimpleTextUtil.writeNewline(output);

      if (numFiles > 0) {
        for(String fileName : files) {
          SimpleTextUtil.write(output, SI_FILE);
          SimpleTextUtil.write(output, fileName, scratch);
          SimpleTextUtil.writeNewline(output);
        }
      }
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(output);
        try {
          dir.deleteFile(segFileName);
        } catch (Throwable t) {
        }
      } else {
        output.close();
      }
    }
  }
}
