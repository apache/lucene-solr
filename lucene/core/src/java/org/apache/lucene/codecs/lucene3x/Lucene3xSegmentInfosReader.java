package org.apache.lucene.codecs.lucene3x;

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
import java.util.Arrays;                          // nocommit
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.SegmentInfosReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * Lucene 3x implementation of {@link SegmentInfosReader}.
 * @lucene.experimental
 * @deprecated
 */
@Deprecated
public class Lucene3xSegmentInfosReader extends SegmentInfosReader {

  public static void readLegacyInfos(SegmentInfos infos, Directory directory, IndexInput input, int format) throws IOException {
    infos.version = input.readLong(); // read version
    infos.counter = input.readInt(); // read counter
    Lucene3xSegmentInfosReader reader = new Lucene3xSegmentInfosReader();
    for (int i = input.readInt(); i > 0; i--) { // read segmentInfos
      SegmentInfo si = reader.readSegmentInfo(null, directory, format, input);
      if (si.getVersion() == null) {
        // Could be a 3.0 - try to open the doc stores - if it fails, it's a
        // 2.x segment, and an IndexFormatTooOldException will be thrown,
        // which is what we want.
        Directory dir = directory;
        if (si.getDocStoreOffset() != -1) {
          if (si.getDocStoreIsCompoundFile()) {
            dir = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(
                si.getDocStoreSegment(), "",
                Lucene3xCodec.COMPOUND_FILE_STORE_EXTENSION), IOContext.READONCE, false);
          }
        } else if (si.getUseCompoundFile()) {
          dir = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(
              si.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION), IOContext.READONCE, false);
        }

        try {
          Lucene3xStoredFieldsReader.checkCodeVersion(dir, si.getDocStoreSegment());
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

  @Override
  public SegmentInfo read(Directory directory, String segmentName, IOContext context) throws IOException { 
    return read(directory, segmentName, Lucene3xSegmentInfosFormat.FORMAT_4X_UPGRADE, context);
  }

  public SegmentInfo read(Directory directory, String segmentName, int format, IOContext context) throws IOException { 

    // NOTE: this is NOT how 3.x is really written...
    String fileName = IndexFileNames.segmentFileName(segmentName, "", Lucene3xSegmentInfosFormat.SI_EXTENSION);

    boolean success = false;

    IndexInput input = directory.openInput(fileName, context);

    try {
      SegmentInfo si = readSegmentInfo(segmentName, directory, format, input);
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

  private static void addIfExists(Directory dir, Set<String> files, String fileName) throws IOException {
    if (dir.fileExists(fileName)) {
      files.add(fileName);
    }
  }

  private SegmentInfo readSegmentInfo(String segmentName, Directory dir, int format, IndexInput input) throws IOException {
    // check that it is a format we can understand
    if (format > Lucene3xSegmentInfosFormat.FORMAT_DIAGNOSTICS) {
      throw new IndexFormatTooOldException(input, format,
                                           Lucene3xSegmentInfosFormat.FORMAT_DIAGNOSTICS, Lucene3xSegmentInfosFormat.FORMAT_4X_UPGRADE);
    }
    if (format < Lucene3xSegmentInfosFormat.FORMAT_4X_UPGRADE) {
      throw new IndexFormatTooNewException(input, format,
                                           Lucene3xSegmentInfosFormat.FORMAT_DIAGNOSTICS, Lucene3xSegmentInfosFormat.FORMAT_4X_UPGRADE);
    }
    final String version;
    if (format <= Lucene3xSegmentInfosFormat.FORMAT_3_1) {
      version = input.readString();
    } else {
      version = null;
    }

    // NOTE: we ignore this and use the incoming arg
    // instead, if it's non-null:
    final String name = input.readString();
    if (segmentName == null) {
      segmentName = name;
    }

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

    // pre-4.0 indexes write a byte if there is a single norms file
    byte b = input.readByte();

    //System.out.println("version=" + version + " name=" + name + " docCount=" + docCount + " delGen=" + delGen + " dso=" + docStoreOffset + " dss=" + docStoreSegment + " dssCFs=" + docStoreIsCompoundFile + " b=" + b + " format=" + format);

    assert 1 == b : "expected 1 but was: "+ b + " format: " + format;
    final int numNormGen = input.readInt();
    final Map<Integer,Long> normGen;
    if (numNormGen == SegmentInfo.NO) {
      normGen = null;
    } else {
      normGen = new HashMap<Integer, Long>();
      for(int j=0;j<numNormGen;j++) {
        normGen.put(j, input.readLong());
      }
    }
    final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;

    final int delCount = input.readInt();
    assert delCount <= docCount;

    final boolean hasProx = input.readByte() == 1;

    final Map<String,String> diagnostics = input.readStringStringMap();

    // nocommit unused...
    final int hasVectors;
    if (format <= Lucene3xSegmentInfosFormat.FORMAT_HAS_VECTORS) {
      hasVectors = input.readByte();
    } else {
      hasVectors = -1;
    }

    final Set<String> files;
    if (format == Lucene3xSegmentInfosFormat.FORMAT_4X_UPGRADE) {
      files = input.readStringSet();
    } else {
      // Replicate logic from 3.x's SegmentInfo.files():
      files = new HashSet<String>();
      if (isCompoundFile) {
        files.add(IndexFileNames.segmentFileName(name, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
      } else {
        addIfExists(dir, files, IndexFileNames.segmentFileName(segmentName, "", "fnm"));
        addIfExists(dir, files, IndexFileNames.segmentFileName(segmentName, "", "frq"));
        addIfExists(dir, files, IndexFileNames.segmentFileName(segmentName, "", "prx"));
        addIfExists(dir, files, IndexFileNames.segmentFileName(segmentName, "", "tis"));
        addIfExists(dir, files, IndexFileNames.segmentFileName(segmentName, "", "tii"));
        addIfExists(dir, files, IndexFileNames.segmentFileName(segmentName, "", "nrm"));
      }

      if (docStoreOffset != -1) {
        if (docStoreIsCompoundFile) {
          files.add(IndexFileNames.segmentFileName(docStoreSegment, "", "cfx"));
        } else {
          files.add(IndexFileNames.segmentFileName(docStoreSegment, "", "fdx"));
          files.add(IndexFileNames.segmentFileName(docStoreSegment, "", "fdt"));
          addIfExists(dir, files, IndexFileNames.segmentFileName(docStoreSegment, "", "tvx"));
          addIfExists(dir, files, IndexFileNames.segmentFileName(docStoreSegment, "", "tvf"));
          addIfExists(dir, files, IndexFileNames.segmentFileName(docStoreSegment, "", "tvd"));
        }
      } else if (!isCompoundFile) {
        files.add(IndexFileNames.segmentFileName(segmentName, "", "fdx"));
        files.add(IndexFileNames.segmentFileName(segmentName, "", "fdt"));
        addIfExists(dir, files, IndexFileNames.segmentFileName(segmentName, "", "tvx"));
        addIfExists(dir, files, IndexFileNames.segmentFileName(segmentName, "", "tvf"));
        addIfExists(dir, files, IndexFileNames.segmentFileName(segmentName, "", "tvd"));
      }

      if (normGen != null) {
        for(Map.Entry<Integer,Long> ent : normGen.entrySet()) {
          long gen = ent.getValue();
          if (gen >= SegmentInfo.YES) {
            // Definitely a separate norm file, with generation:
            files.add(IndexFileNames.fileNameFromGeneration(segmentName, "s" + ent.getKey(), gen));
          } else if (gen == SegmentInfo.NO) {
            // No seaprate norm
          } else {
            // nocommit -- i thought _X_N.sY files were pre-3.0...????
            assert false;
            /*
            System.out.println("FILES: " + Arrays.toString(dir.listAll()) + "; seg=" + segmentName);
            addIfExists(dir, files, IndexFileNames.fileNameFromGeneration(segmentName, "s" + ent.getKey(), gen));
            assert false: "gen=" + gen;
            */
          }
        }
      }
    }

    // nocommit we can use hasProx/hasVectors from the 3.x
    // si... if we can pass this to the other components...?

    SegmentInfo info = new SegmentInfo(dir, version, segmentName, docCount, docStoreOffset,
                                       docStoreSegment, docStoreIsCompoundFile, normGen, isCompoundFile,
                                       delCount, null, diagnostics);
    info.setDelGen(delGen);
    info.setFiles(files);
    return info;
  }
}
