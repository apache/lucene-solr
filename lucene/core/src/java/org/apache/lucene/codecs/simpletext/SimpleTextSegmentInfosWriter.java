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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.codecs.SegmentInfosWriter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
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
public class SimpleTextSegmentInfosWriter extends SegmentInfosWriter {

  final static BytesRef VERSION             = new BytesRef("version ");
  final static BytesRef COUNTER             = new BytesRef("counter ");
  final static BytesRef NUM_USERDATA        = new BytesRef("user data entries ");
  final static BytesRef USERDATA_KEY        = new BytesRef("  key ");
  final static BytesRef USERDATA_VALUE      = new BytesRef("  value ");
  final static BytesRef NUM_SEGMENTS        = new BytesRef("number of segments ");
  final static BytesRef SI_NAME             = new BytesRef("  name ");
  final static BytesRef SI_CODEC            = new BytesRef("    codec ");
  final static BytesRef SI_VERSION          = new BytesRef("    version ");
  final static BytesRef SI_DOCCOUNT         = new BytesRef("    number of documents ");
  final static BytesRef SI_DELCOUNT         = new BytesRef("    number of deletions ");
  final static BytesRef SI_HASPROX          = new BytesRef("    has prox ");
  final static BytesRef SI_HASVECTORS       = new BytesRef("    has vectors ");
  final static BytesRef SI_USECOMPOUND      = new BytesRef("    uses compound file ");
  final static BytesRef SI_DSOFFSET         = new BytesRef("    docstore offset ");
  final static BytesRef SI_DSSEGMENT        = new BytesRef("    docstore segment ");
  final static BytesRef SI_DSCOMPOUND       = new BytesRef("    docstore is compound file ");
  final static BytesRef SI_DELGEN           = new BytesRef("    deletion generation ");
  final static BytesRef SI_NUM_NORMGEN      = new BytesRef("    norms generations ");
  final static BytesRef SI_NORMGEN_KEY      = new BytesRef("      key ");
  final static BytesRef SI_NORMGEN_VALUE    = new BytesRef("      value ");
  final static BytesRef SI_NUM_DIAG         = new BytesRef("    diagnostics ");
  final static BytesRef SI_DIAG_KEY         = new BytesRef("      key ");
  final static BytesRef SI_DIAG_VALUE       = new BytesRef("      value ");
  
  @Override
  public IndexOutput writeInfos(Directory dir, String segmentsFileName, String codecID, SegmentInfos infos, IOContext context) throws IOException {
    BytesRef scratch = new BytesRef();
    IndexOutput out = new ChecksumIndexOutput(dir.createOutput(segmentsFileName, new IOContext(new FlushInfo(infos.size(), infos.totalDocCount()))));
    boolean success = false;
    try {
      // required preamble:
      out.writeInt(SegmentInfos.FORMAT_CURRENT); // write FORMAT
      out.writeString(codecID); // write codecID
      // end preamble
      
      // version
      SimpleTextUtil.write(out, VERSION);
      SimpleTextUtil.write(out, Long.toString(infos.version), scratch);
      SimpleTextUtil.writeNewline(out);

      // counter
      SimpleTextUtil.write(out, COUNTER);
      SimpleTextUtil.write(out, Integer.toString(infos.counter), scratch);
      SimpleTextUtil.writeNewline(out);

      // user data
      int numUserDataEntries = infos.getUserData() == null ? 0 : infos.getUserData().size();
      SimpleTextUtil.write(out, NUM_USERDATA);
      SimpleTextUtil.write(out, Integer.toString(numUserDataEntries), scratch);
      SimpleTextUtil.writeNewline(out);
      
      if (numUserDataEntries > 0) {
        for (Map.Entry<String,String> userEntry : infos.getUserData().entrySet()) {
          SimpleTextUtil.write(out, USERDATA_KEY);
          SimpleTextUtil.write(out, userEntry.getKey(), scratch);
          SimpleTextUtil.writeNewline(out);
          
          SimpleTextUtil.write(out, USERDATA_VALUE);
          SimpleTextUtil.write(out, userEntry.getValue(), scratch);
          SimpleTextUtil.writeNewline(out);
        }
      }
      
      // infos size
      SimpleTextUtil.write(out, NUM_SEGMENTS);
      SimpleTextUtil.write(out, Integer.toString(infos.size()), scratch);
      SimpleTextUtil.writeNewline(out);

      for (SegmentInfo si : infos) {
        writeInfo(out, si);
      } 

      success = true;
      return out;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }
  
  private void writeInfo(IndexOutput output, SegmentInfo si) throws IOException {
    assert si.getDelCount() <= si.docCount: "delCount=" + si.getDelCount() + " docCount=" + si.docCount + " segment=" + si.name;
    BytesRef scratch = new BytesRef();
    
    SimpleTextUtil.write(output, SI_NAME);
    SimpleTextUtil.write(output, si.name, scratch);
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_CODEC);
    SimpleTextUtil.write(output, si.getCodec().getName(), scratch);
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_VERSION);
    SimpleTextUtil.write(output, si.getVersion(), scratch);
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_DOCCOUNT);
    SimpleTextUtil.write(output, Integer.toString(si.docCount), scratch);
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_DELCOUNT);
    SimpleTextUtil.write(output, Integer.toString(si.getDelCount()), scratch);
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_HASPROX);
    switch(si.getHasProxInternal()) {
      case SegmentInfo.YES: SimpleTextUtil.write(output, "true", scratch); break;
      case SegmentInfo.CHECK_FIELDINFO: SimpleTextUtil.write(output, "check fieldinfo", scratch); break;
      // its "NO" if its 'anything but YES'... such as 0
      default: SimpleTextUtil.write(output, "false", scratch); break;
    }
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_HASVECTORS);
    switch(si.getHasVectorsInternal()) {
      case SegmentInfo.YES: SimpleTextUtil.write(output, "true", scratch); break;
      case SegmentInfo.CHECK_FIELDINFO: SimpleTextUtil.write(output, "check fieldinfo", scratch); break;
      // its "NO" if its 'anything but YES'... such as 0
      default: SimpleTextUtil.write(output, "false", scratch); break;
    }
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_USECOMPOUND);
    SimpleTextUtil.write(output, Boolean.toString(si.getUseCompoundFile()), scratch);
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_DSOFFSET);
    SimpleTextUtil.write(output, Integer.toString(si.getDocStoreOffset()), scratch);
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_DSSEGMENT);
    SimpleTextUtil.write(output, si.getDocStoreSegment(), scratch);
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_DSCOMPOUND);
    SimpleTextUtil.write(output, Boolean.toString(si.getDocStoreIsCompoundFile()), scratch);
    SimpleTextUtil.writeNewline(output);
    
    SimpleTextUtil.write(output, SI_DELGEN);
    SimpleTextUtil.write(output, Long.toString(si.getDelGen()), scratch);
    SimpleTextUtil.writeNewline(output);
    
    Map<Integer,Long> normGen = si.getNormGen();
    int numNormGen = normGen == null ? 0 : normGen.size();
    SimpleTextUtil.write(output, SI_NUM_NORMGEN);
    SimpleTextUtil.write(output, Integer.toString(numNormGen), scratch);
    SimpleTextUtil.writeNewline(output);
    
    if (numNormGen > 0) {
      for (Entry<Integer,Long> entry : normGen.entrySet()) {
        SimpleTextUtil.write(output, SI_NORMGEN_KEY);
        SimpleTextUtil.write(output, Integer.toString(entry.getKey()), scratch);
        SimpleTextUtil.writeNewline(output);
        
        SimpleTextUtil.write(output, SI_NORMGEN_VALUE);
        SimpleTextUtil.write(output, Long.toString(entry.getValue()), scratch);
        SimpleTextUtil.writeNewline(output);
      }
    }
    
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
  }

  @Override
  public void prepareCommit(IndexOutput out) throws IOException {
    ((ChecksumIndexOutput)out).prepareCommit();
  }

  @Override
  public void finishCommit(IndexOutput out) throws IOException {
    ((ChecksumIndexOutput)out).finishCommit();
    out.close();
  }
}
