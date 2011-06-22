package org.apache.lucene.index.codecs;

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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldsReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;

/**
 * Default implementation of {@link SegmentInfosReader}.
 * @lucene.experimental
 */
public class DefaultSegmentInfosReader extends SegmentInfosReader {

  @Override
  public void read(Directory directory, String segmentsFileName, CodecProvider codecs,
          SegmentInfos infos) throws IOException {
    IndexInput input = null;
    try {
      input = openInput(directory, segmentsFileName);
      final int format = input.readInt();
      infos.setFormat(format);
  
      // check that it is a format we can understand
      if (format > DefaultSegmentInfosWriter.FORMAT_MINIMUM)
        throw new IndexFormatTooOldException(segmentsFileName, format,
          DefaultSegmentInfosWriter.FORMAT_MINIMUM, DefaultSegmentInfosWriter.FORMAT_CURRENT);
      if (format < DefaultSegmentInfosWriter.FORMAT_CURRENT)
        throw new IndexFormatTooNewException(segmentsFileName, format,
          DefaultSegmentInfosWriter.FORMAT_MINIMUM, DefaultSegmentInfosWriter.FORMAT_CURRENT);
  
      infos.version = input.readLong(); // read version
      infos.counter = input.readInt(); // read counter
      if (infos.getFormat() <= DefaultSegmentInfosWriter.FORMAT_4_0) {
        infos.setGlobalFieldMapVersion(input.readLong());
      }
      for (int i = input.readInt(); i > 0; i--) { // read segmentInfos
        SegmentInfo si = new SegmentInfo(directory, format, input, codecs);
        if (si.getVersion() == null) {
          // Could be a 3.0 - try to open the doc stores - if it fails, it's a
          // 2.x segment, and an IndexFormatTooOldException will be thrown,
          // which is what we want.
          Directory dir = directory;
          if (si.getDocStoreOffset() != -1) {
            if (si.getDocStoreIsCompoundFile()) {
              dir = dir.openCompoundInput(IndexFileNames.segmentFileName(
                  si.getDocStoreSegment(), "",
                  IndexFileNames.COMPOUND_FILE_STORE_EXTENSION), 1024);
            }
          } else if (si.getUseCompoundFile()) {
            dir = dir.openCompoundInput(IndexFileNames.segmentFileName(
                si.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION), 1024);
          }

          try {
            FieldsReader.checkCodeVersion(dir, si.getDocStoreSegment());
          } finally {
            // If we opened the directory, close it
            if (dir != directory) dir.close();
          }
          
          // Above call succeeded, so it's a 3.0 segment. Upgrade it so the next
          // time the segment is read, its version won't be null and we won't
          // need to open FieldsReader every time for each such segment.
          si.setVersion("3.0");
        } else if (si.getVersion().equals("2.x")) {
          // If it's a 3x index touched by 3.1+ code, then segments record their
          // version, whether they are 2.x ones or not. We detect that and throw
          // appropriate exception.
          throw new IndexFormatTooOldException(si.name, si.getVersion());
        }
        infos.add(si);
      }
      
      infos.userData = input.readStringStringMap();
      finalizeInput(input);
      
    } finally {
      if (input != null) {
        input.close();
      }
    }

  }
  
  public IndexInput openInput(Directory dir, String segmentsFileName) throws IOException {
    IndexInput in = dir.openInput(segmentsFileName);
    return new ChecksumIndexInput(in);
    
  }
  
  public void finalizeInput(IndexInput input) throws IOException, CorruptIndexException {
    ChecksumIndexInput cksumInput = (ChecksumIndexInput)input;
    final long checksumNow = cksumInput.getChecksum();
    final long checksumThen = cksumInput.readLong();
    if (checksumNow != checksumThen)
      throw new CorruptIndexException("checksum mismatch in segments file");
    
  }

}
