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
package org.apache.lucene.codecs.lucene50;


import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo; // javadocs
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;

/**
 * Read-write version of 5.0 SegmentInfoFormat for testing
 * @deprecated for test purposes only
 */
@Deprecated
public class Lucene50RWSegmentInfoFormat extends Lucene50SegmentInfoFormat {

  /** Sole constructor. */
  public Lucene50RWSegmentInfoFormat() {
  }
  
  @Override
  public SegmentInfo read(Directory dir, String segment, byte[] segmentID, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segment, "", Lucene50SegmentInfoFormat.SI_EXTENSION);
    try (ChecksumIndexInput input = dir.openChecksumInput(fileName, context)) {
      Throwable priorE = null;
      SegmentInfo si = null;
      try {
        CodecUtil.checkIndexHeader(input, Lucene50SegmentInfoFormat.CODEC_NAME,
                                          Lucene50SegmentInfoFormat.VERSION_START,
                                          Lucene50SegmentInfoFormat.VERSION_CURRENT,
                                          segmentID, "");
        final Version version = Version.fromBits(input.readInt(), input.readInt(), input.readInt());
        
        final int docCount = input.readInt();
        if (docCount < 0) {
          throw new CorruptIndexException("invalid docCount: " + docCount, input);
        }
        final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;
        
        final Map<String,String> diagnostics = input.readMapOfStrings();
        final Set<String> files = input.readSetOfStrings();
        final Map<String,String> attributes = input.readMapOfStrings();
        
        si = new SegmentInfo(dir, version, null, segment, docCount, isCompoundFile, null, diagnostics, segmentID, attributes, null);
        si.setFiles(files);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(input, priorE);
      }
      return si;
    }
  }

  @Override
  public void write(Directory dir, SegmentInfo si, IOContext ioContext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(si.name, "", Lucene50SegmentInfoFormat.SI_EXTENSION);

    assert si.getIndexSort() == null;

    try (IndexOutput output = dir.createOutput(fileName, ioContext)) {
      // Only add the file once we've successfully created it, else IFD assert can trip:
      si.addFile(fileName);
      CodecUtil.writeIndexHeader(output, 
                                   Lucene50SegmentInfoFormat.CODEC_NAME, 
                                   Lucene50SegmentInfoFormat.VERSION_CURRENT,
                                   si.getId(),
                                   "");
      Version version = si.getVersion();
      if (version.major < 5) {
        throw new IllegalArgumentException("invalid major version: should be >= 5 but got: " + version.major + " segment=" + si);
      }
      // Write the Lucene version that created this segment, since 3.1
      output.writeInt(version.major);
      output.writeInt(version.minor);
      output.writeInt(version.bugfix);
      assert version.prerelease == 0;
      output.writeInt(si.maxDoc());

      output.writeByte((byte) (si.getUseCompoundFile() ? SegmentInfo.YES : SegmentInfo.NO));
      output.writeMapOfStrings(si.getDiagnostics());
      Set<String> files = si.files();
      for (String file : files) {
        if (!IndexFileNames.parseSegmentName(file).equals(si.name)) {
          throw new IllegalArgumentException("invalid files: expected segment=" + si.name + ", got=" + files);
        }
      }
      output.writeSetOfStrings(files);
      output.writeMapOfStrings(si.getAttributes());
      
      CodecUtil.writeFooter(output);
    }
  }

  /** File extension used to store {@link SegmentInfo}. */
  public final static String SI_EXTENSION = "si";
  static final String CODEC_NAME = "Lucene50SegmentInfo";
  static final int VERSION_SAFE_MAPS = 1;
  static final int VERSION_START = VERSION_SAFE_MAPS;
  static final int VERSION_CURRENT = VERSION_SAFE_MAPS;
}
