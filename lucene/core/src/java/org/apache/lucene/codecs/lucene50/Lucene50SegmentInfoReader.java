package org.apache.lucene.codecs.lucene50;

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
import java.text.ParseException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.SegmentInfoReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;

/**
 * Lucene 5.0 implementation of {@link SegmentInfoReader}.
 * 
 * @see Lucene50SegmentInfoFormat
 * @lucene.experimental
 */
public class Lucene50SegmentInfoReader extends SegmentInfoReader {

  /** Sole constructor. */
  public Lucene50SegmentInfoReader() {
  }

  @Override
  public SegmentInfo read(Directory dir, String segment, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segment, "", Lucene50SegmentInfoFormat.SI_EXTENSION);
    try (ChecksumIndexInput input = dir.openChecksumInput(fileName, context)) {
      Throwable priorE = null;
      SegmentInfo si = null;
      try {
        CodecUtil.checkHeader(input, Lucene50SegmentInfoFormat.CODEC_NAME,
                                     Lucene50SegmentInfoFormat.VERSION_START,
                                     Lucene50SegmentInfoFormat.VERSION_CURRENT);
        final Version version;
        try {
          version = Version.parse(input.readString());
        } catch (ParseException pe) {
          throw new CorruptIndexException("unable to parse version string: " + pe.getMessage(), input, pe);
        }
        
        final int docCount = input.readInt();
        if (docCount < 0) {
          throw new CorruptIndexException("invalid docCount: " + docCount, input);
        }
        final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;
        final Map<String,String> diagnostics = input.readStringStringMap();
        final Set<String> files = input.readStringSet();
        
        String id = input.readString();
        
        si = new SegmentInfo(dir, version, segment, docCount, isCompoundFile, null, diagnostics, id);
        si.setFiles(files);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(input, priorE);
      }
      return si;
    }
  }
}
