package org.apache.lucene.codecs.lucene40;

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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.SegmentInfoReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * Lucene 4.0 implementation of {@link SegmentInfoReader}.
 * 
 * @see Lucene40SegmentInfoFormat
 * @lucene.experimental
 */
public class Lucene40SegmentInfoReader extends SegmentInfoReader {

  /** Sole constructor. */
  public Lucene40SegmentInfoReader() {
  }

  @Override
  public SegmentInfo read(Directory dir, String segment, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segment, "", Lucene40SegmentInfoFormat.SI_EXTENSION);
    final IndexInput input = dir.openInput(fileName, context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40SegmentInfoFormat.CODEC_NAME,
                                   Lucene40SegmentInfoFormat.VERSION_START,
                                   Lucene40SegmentInfoFormat.VERSION_CURRENT);
      final String version = input.readString();
      final int docCount = input.readInt();
      if (docCount < 0) {
        throw new CorruptIndexException("invalid docCount: " + docCount + " (resource=" + input + ")");
      }
      final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;
      final Map<String,String> diagnostics = input.readStringStringMap();
      final Map<String,String> attributes = input.readStringStringMap();
      final Set<String> files = input.readStringSet();
      
      if (input.getFilePointer() != input.length()) {
        throw new CorruptIndexException("did not read all bytes from file \"" + fileName + "\": read " + input.getFilePointer() + " vs size " + input.length() + " (resource: " + input + ")");
      }

      final SegmentInfo si = new SegmentInfo(dir, version, segment, docCount, isCompoundFile,
                                             null, diagnostics, Collections.unmodifiableMap(attributes));
      si.setFiles(files);

      success = true;

      return si;

    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(input);
      } else {
        input.close();
      }
    }
  }
}
