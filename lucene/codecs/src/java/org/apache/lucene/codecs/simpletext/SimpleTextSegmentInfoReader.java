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

import static org.apache.lucene.codecs.simpletext.SimpleTextSegmentInfoWriter.SI_DIAG_KEY;
import static org.apache.lucene.codecs.simpletext.SimpleTextSegmentInfoWriter.SI_DIAG_VALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextSegmentInfoWriter.SI_DOCCOUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextSegmentInfoWriter.SI_FILE;
import static org.apache.lucene.codecs.simpletext.SimpleTextSegmentInfoWriter.SI_NUM_DIAG;
import static org.apache.lucene.codecs.simpletext.SimpleTextSegmentInfoWriter.SI_NUM_FILES;
import static org.apache.lucene.codecs.simpletext.SimpleTextSegmentInfoWriter.SI_USECOMPOUND;
import static org.apache.lucene.codecs.simpletext.SimpleTextSegmentInfoWriter.SI_VERSION;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.SegmentInfoReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

/**
 * reads plaintext segments files
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextSegmentInfoReader extends SegmentInfoReader {

  @Override
  public SegmentInfo read(Directory directory, String segmentName, IOContext context) throws IOException {
    BytesRef scratch = new BytesRef();
    String segFileName = IndexFileNames.segmentFileName(segmentName, "", SimpleTextSegmentInfoFormat.SI_EXTENSION);
    IndexInput input = directory.openInput(segFileName, context);
    boolean success = false;
    try {
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, SI_VERSION);
      final String version = readString(SI_VERSION.length, scratch);
    
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, SI_DOCCOUNT);
      final int docCount = Integer.parseInt(readString(SI_DOCCOUNT.length, scratch));
    
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, SI_USECOMPOUND);
      final boolean isCompoundFile = Boolean.parseBoolean(readString(SI_USECOMPOUND.length, scratch));
    
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, SI_NUM_DIAG);
      int numDiag = Integer.parseInt(readString(SI_NUM_DIAG.length, scratch));
      Map<String,String> diagnostics = new HashMap<String,String>();

      for (int i = 0; i < numDiag; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, SI_DIAG_KEY);
        String key = readString(SI_DIAG_KEY.length, scratch);
      
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, SI_DIAG_VALUE);
        String value = readString(SI_DIAG_VALUE.length, scratch);
        diagnostics.put(key, value);
      }
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, SI_NUM_FILES);
      int numFiles = Integer.parseInt(readString(SI_NUM_FILES.length, scratch));
      Set<String> files = new HashSet<String>();

      for (int i = 0; i < numFiles; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, SI_FILE);
        String fileName = readString(SI_FILE.length, scratch);
        files.add(fileName);
      }

      SegmentInfo info = new SegmentInfo(directory, version, segmentName, docCount, 
                                         isCompoundFile, null, diagnostics);
      info.setFiles(files);
      success = true;
      return info;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(input);
      } else {
        input.close();
      }
    }
  }

  private String readString(int offset, BytesRef scratch) {
    return new String(scratch.bytes, scratch.offset+offset, scratch.length-offset, IOUtils.CHARSET_UTF_8);
  }
}
