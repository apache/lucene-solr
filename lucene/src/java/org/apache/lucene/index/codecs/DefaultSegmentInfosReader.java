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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Default implementation of {@link SegmentInfosReader}.
 * @lucene.experimental
 */
public class DefaultSegmentInfosReader extends SegmentInfosReader {

  // TODO: shove all backwards code to preflex!
  // this is a little tricky, because of IR.commit(), two options:
  // 1. PreFlex writes 4.x SIS format, but reads both 3.x and 4.x
  //    (and maybe RW always only writes the 3.x one? for that to work well,
  //     we have to move .fnx file to codec too, not too bad but more work).
  //     or we just have crappier RW testing like today.
  // 2. PreFlex writes 3.x SIS format, and only reads 3.x
  //    (in this case we have to move .fnx file to codec as well)
  @Override
  public void read(Directory directory, String segmentsFileName, ChecksumIndexInput input, SegmentInfos infos, IOContext context) throws IOException { 
    infos.version = input.readLong(); // read version
    infos.counter = input.readInt(); // read counter
    final int format = infos.getFormat();
    if (format <= DefaultSegmentInfosWriter.FORMAT_4_0) {
      infos.setGlobalFieldMapVersion(input.readLong());
    }
    for (int i = input.readInt(); i > 0; i--) { // read segmentInfos
      SegmentInfo si = readSegmentInfo(directory, format, input);
      if (si.getVersion() == null) {
        // Could be a 3.0 - try to open the doc stores - if it fails, it's a
        // 2.x segment, and an IndexFormatTooOldException will be thrown,
        // which is what we want.
        Directory dir = directory;
        if (si.getDocStoreOffset() != -1) {
          if (si.getDocStoreIsCompoundFile()) {
            dir = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(
                si.getDocStoreSegment(), "",
                IndexFileNames.COMPOUND_FILE_STORE_EXTENSION), context, false);
          }
        } else if (si.getUseCompoundFile()) {
          dir = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(
              si.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION), context, false);
        }

        try {
          DefaultStoredFieldsReader.checkCodeVersion(dir, si.getDocStoreSegment());
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
        throw new IndexFormatTooOldException("segment " + si.name + " in resource " + input, si.getVersion());
      }
      infos.add(si);
    }
      
    infos.userData = input.readStringStringMap();
  }
  
  // if we make a preflex impl we can remove a lot of this hair...
  public SegmentInfo readSegmentInfo(Directory dir, int format, ChecksumIndexInput input) throws IOException {
    final String version;
    if (format <= DefaultSegmentInfosWriter.FORMAT_3_1) {
      version = input.readString();
    } else {
      version = null;
    }
    final String name = input.readString();
    final int docCount = input.readInt();
    final long delGen = input.readLong();
    final int docStoreOffset = input.readInt();
    final String docStoreSegment;
    final boolean docStoreIsCompoundFile;
    if (docStoreOffset != -1) {
      docStoreSegment = input.readString();
      docStoreIsCompoundFile = input.readByte() == SegmentInfo.YES;
    } else {
      docStoreSegment = name;
      docStoreIsCompoundFile = false;
    }

    if (format > DefaultSegmentInfosWriter.FORMAT_4_0) {
      // pre-4.0 indexes write a byte if there is a single norms file
      byte b = input.readByte();
      assert 1 == b;
    }

    final int numNormGen = input.readInt();
    final Map<Integer,Long> normGen;
    if (numNormGen == SegmentInfo.NO) {
      normGen = null;
    } else {
      normGen = new HashMap<Integer, Long>();
      for(int j=0;j<numNormGen;j++) {
        int fieldNumber = j;
        if (format <= DefaultSegmentInfosWriter.FORMAT_4_0) {
          fieldNumber = input.readInt();
        }

        normGen.put(fieldNumber, input.readLong());
      }
    }
    final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;

    final int delCount = input.readInt();
    assert delCount <= docCount;

    final int hasProx = input.readByte();

    final Codec codec;
    // note: if the codec is not available: Codec.forName will throw an exception.
    if (format <= DefaultSegmentInfosWriter.FORMAT_4_0) {
      codec = Codec.forName(input.readString());
    } else {
      codec = Codec.forName("Lucene3x");
    }
    final Map<String,String> diagnostics = input.readStringStringMap();

    final int hasVectors;
    if (format <= DefaultSegmentInfosWriter.FORMAT_HAS_VECTORS) {
      hasVectors = input.readByte();
    } else {
      final String storesSegment;
      final String ext;
      final boolean storeIsCompoundFile;
      if (docStoreOffset != -1) {
        storesSegment = docStoreSegment;
        storeIsCompoundFile = docStoreIsCompoundFile;
        ext = IndexFileNames.COMPOUND_FILE_STORE_EXTENSION;
      } else {
        storesSegment = name;
        storeIsCompoundFile = isCompoundFile;
        ext = IndexFileNames.COMPOUND_FILE_EXTENSION;
      }
      final Directory dirToTest;
      if (storeIsCompoundFile) {
        dirToTest = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(storesSegment, "", ext), IOContext.READONCE, false);
      } else {
        dirToTest = dir;
      }
      try {
        // TODO: remove this manual file check or push to preflex codec
        hasVectors = dirToTest.fileExists(IndexFileNames.segmentFileName(storesSegment, "", DefaultTermVectorsReader.VECTORS_INDEX_EXTENSION)) ? SegmentInfo.YES : SegmentInfo.NO;
      } finally {
        if (isCompoundFile) {
          dirToTest.close();
        }
      }
    }
    
    return new SegmentInfo(dir, version, name, docCount, delGen, docStoreOffset,
      docStoreSegment, docStoreIsCompoundFile, normGen, isCompoundFile,
      delCount, hasProx, codec, diagnostics, hasVectors);
  }
}
