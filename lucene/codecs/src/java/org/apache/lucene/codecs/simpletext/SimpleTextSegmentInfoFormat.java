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
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/**
 * plain text segments file format.
 * <p>
 * <b>FOR RECREATIONAL USE ONLY</b>
 * @lucene.experimental
 */
public class SimpleTextSegmentInfoFormat extends SegmentInfoFormat {
  final static BytesRef SI_VERSION          = new BytesRef("    version ");
  final static BytesRef SI_DOCCOUNT         = new BytesRef("    number of documents ");
  final static BytesRef SI_USECOMPOUND      = new BytesRef("    uses compound file ");
  final static BytesRef SI_NUM_DIAG         = new BytesRef("    diagnostics ");
  final static BytesRef SI_DIAG_KEY         = new BytesRef("      key ");
  final static BytesRef SI_DIAG_VALUE       = new BytesRef("      value ");
  final static BytesRef SI_NUM_ATT          = new BytesRef("    attributes ");
  final static BytesRef SI_ATT_KEY          = new BytesRef("      key ");
  final static BytesRef SI_ATT_VALUE        = new BytesRef("      value ");
  final static BytesRef SI_NUM_FILES        = new BytesRef("    files ");
  final static BytesRef SI_FILE             = new BytesRef("      file ");
  final static BytesRef SI_ID               = new BytesRef("    id ");

  public static final String SI_EXTENSION = "si";
  
  @Override
  public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    String segFileName = IndexFileNames.segmentFileName(segmentName, "", SimpleTextSegmentInfoFormat.SI_EXTENSION);
    try (ChecksumIndexInput input = directory.openChecksumInput(segFileName, context)) {
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_VERSION);
      final Version version;
      try {
        version = Version.parse(readString(SI_VERSION.length, scratch));
      } catch (ParseException pe) {
        throw new CorruptIndexException("unable to parse version string: " + pe.getMessage(), input, pe);
      }
    
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_DOCCOUNT);
      final int docCount = Integer.parseInt(readString(SI_DOCCOUNT.length, scratch));
    
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_USECOMPOUND);
      final boolean isCompoundFile = Boolean.parseBoolean(readString(SI_USECOMPOUND.length, scratch));
    
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_NUM_DIAG);
      int numDiag = Integer.parseInt(readString(SI_NUM_DIAG.length, scratch));
      Map<String,String> diagnostics = new HashMap<>();

      for (int i = 0; i < numDiag; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_DIAG_KEY);
        String key = readString(SI_DIAG_KEY.length, scratch);
      
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_DIAG_VALUE);
        String value = readString(SI_DIAG_VALUE.length, scratch);
        diagnostics.put(key, value);
      }
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_NUM_ATT);
      int numAtt = Integer.parseInt(readString(SI_NUM_ATT.length, scratch));
      Map<String,String> attributes = new HashMap<>(numAtt);

      for (int i = 0; i < numAtt; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_ATT_KEY);
        String key = readString(SI_ATT_KEY.length, scratch);
      
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_ATT_VALUE);
        String value = readString(SI_ATT_VALUE.length, scratch);
        attributes.put(key, value);
      }
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_NUM_FILES);
      int numFiles = Integer.parseInt(readString(SI_NUM_FILES.length, scratch));
      Set<String> files = new HashSet<>();

      for (int i = 0; i < numFiles; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch.get(), SI_FILE);
        String fileName = readString(SI_FILE.length, scratch);
        files.add(fileName);
      }
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch.get(), SI_ID);
      final byte[] id = Arrays.copyOfRange(scratch.bytes(), SI_ID.length, scratch.length());
      
      if (!Arrays.equals(segmentID, id)) {
        throw new CorruptIndexException("file mismatch, expected: " + StringHelper.idToString(segmentID)
                                                        + ", got: " + StringHelper.idToString(id), input);
      }

      SimpleTextUtil.checkFooter(input);

      SegmentInfo info = new SegmentInfo(directory, version, segmentName, docCount,
                                         isCompoundFile, null, Collections.unmodifiableMap(diagnostics), id, Collections.unmodifiableMap(attributes));
      info.setFiles(files);
      return info;
    }
  }

  private String readString(int offset, BytesRefBuilder scratch) {
    return new String(scratch.bytes(), offset, scratch.length()-offset, StandardCharsets.UTF_8);
  }
  
  @Override
  public void write(Directory dir, SegmentInfo si, IOContext ioContext) throws IOException {

    String segFileName = IndexFileNames.segmentFileName(si.name, "", SimpleTextSegmentInfoFormat.SI_EXTENSION);

    try (IndexOutput output = dir.createOutput(segFileName, ioContext)) {
      // Only add the file once we've successfully created it, else IFD assert can trip:
      si.addFile(segFileName);
      BytesRefBuilder scratch = new BytesRefBuilder();
    
      SimpleTextUtil.write(output, SI_VERSION);
      SimpleTextUtil.write(output, si.getVersion().toString(), scratch);
      SimpleTextUtil.writeNewline(output);
    
      SimpleTextUtil.write(output, SI_DOCCOUNT);
      SimpleTextUtil.write(output, Integer.toString(si.maxDoc()), scratch);
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
      
      Map<String,String> attributes = si.getAttributes();
      SimpleTextUtil.write(output, SI_NUM_ATT);
      SimpleTextUtil.write(output, Integer.toString(attributes.size()), scratch);
      SimpleTextUtil.writeNewline(output);
    
      for (Map.Entry<String,String> attEntry : attributes.entrySet()) {
        SimpleTextUtil.write(output, SI_ATT_KEY);
        SimpleTextUtil.write(output, attEntry.getKey(), scratch);
        SimpleTextUtil.writeNewline(output);
        
        SimpleTextUtil.write(output, SI_ATT_VALUE);
        SimpleTextUtil.write(output, attEntry.getValue(), scratch);
        SimpleTextUtil.writeNewline(output);
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

      SimpleTextUtil.write(output, SI_ID);
      SimpleTextUtil.write(output, new BytesRef(si.getId()));
      SimpleTextUtil.writeNewline(output);
      
      SimpleTextUtil.writeChecksum(output, scratch);
    }
  }
}
