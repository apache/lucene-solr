package org.apache.lucene.codecs.lucene3x;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.SegmentInfoReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * Lucene 3x implementation of {@link SegmentInfoReader}.
 * @lucene.experimental
 * @deprecated Only for reading existing 3.x indexes
 */
@Deprecated
public class Lucene3xSegmentInfoReader extends SegmentInfoReader {

  public static void readLegacyInfos(SegmentInfos infos, Directory directory, IndexInput input, int format) throws IOException {
    infos.version = input.readLong(); // read version
    infos.counter = input.readInt(); // read counter
    Lucene3xSegmentInfoReader reader = new Lucene3xSegmentInfoReader();
    for (int i = input.readInt(); i > 0; i--) { // read segmentInfos
      SegmentInfoPerCommit siPerCommit = reader.readLegacySegmentInfo(directory, format, input);
      SegmentInfo si = siPerCommit.info;

      if (si.getVersion() == null) {
        // Could be a 3.0 - try to open the doc stores - if it fails, it's a
        // 2.x segment, and an IndexFormatTooOldException will be thrown,
        // which is what we want.
        Directory dir = directory;
        if (Lucene3xSegmentInfoFormat.getDocStoreOffset(si) != -1) {
          if (Lucene3xSegmentInfoFormat.getDocStoreIsCompoundFile(si)) {
            dir = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(
                Lucene3xSegmentInfoFormat.getDocStoreSegment(si), "",
                Lucene3xCodec.COMPOUND_FILE_STORE_EXTENSION), IOContext.READONCE, false);
          }
        } else if (si.getUseCompoundFile()) {
          dir = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(
              si.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION), IOContext.READONCE, false);
        }

        try {
          Lucene3xStoredFieldsReader.checkCodeVersion(dir, Lucene3xSegmentInfoFormat.getDocStoreSegment(si));
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
      infos.add(siPerCommit);
    }
      
    infos.userData = input.readStringStringMap();
  }

  @Override
  public SegmentInfo read(Directory directory, String segmentName, IOContext context) throws IOException { 
    // NOTE: this is NOT how 3.x is really written...
    String fileName = IndexFileNames.segmentFileName(segmentName, "", Lucene3xSegmentInfoFormat.UPGRADED_SI_EXTENSION);

    boolean success = false;

    IndexInput input = directory.openInput(fileName, context);

    try {
      SegmentInfo si = readUpgradedSegmentInfo(segmentName, directory, input);
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
  
  /** reads from legacy 3.x segments_N */
  private SegmentInfoPerCommit readLegacySegmentInfo(Directory dir, int format, IndexInput input) throws IOException {
    // check that it is a format we can understand
    if (format > Lucene3xSegmentInfoFormat.FORMAT_DIAGNOSTICS) {
      throw new IndexFormatTooOldException(input, format,
                                           Lucene3xSegmentInfoFormat.FORMAT_DIAGNOSTICS, Lucene3xSegmentInfoFormat.FORMAT_3_1);
    }
    if (format < Lucene3xSegmentInfoFormat.FORMAT_3_1) {
      throw new IndexFormatTooNewException(input, format,
                                           Lucene3xSegmentInfoFormat.FORMAT_DIAGNOSTICS, Lucene3xSegmentInfoFormat.FORMAT_3_1);
    }
    final String version;
    if (format <= Lucene3xSegmentInfoFormat.FORMAT_3_1) {
      version = input.readString();
    } else {
      version = null;
    }

    final String name = input.readString();

    final int docCount = input.readInt();
    final long delGen = input.readLong();
    
    final int docStoreOffset = input.readInt();
    final Map<String,String> attributes = new HashMap<String,String>();
    
    // parse the docstore stuff and shove it into attributes
    final String docStoreSegment;
    final boolean docStoreIsCompoundFile;
    if (docStoreOffset != -1) {
      docStoreSegment = input.readString();
      docStoreIsCompoundFile = input.readByte() == SegmentInfo.YES;
      attributes.put(Lucene3xSegmentInfoFormat.DS_OFFSET_KEY, Integer.toString(docStoreOffset));
      attributes.put(Lucene3xSegmentInfoFormat.DS_NAME_KEY, docStoreSegment);
      attributes.put(Lucene3xSegmentInfoFormat.DS_COMPOUND_KEY, Boolean.toString(docStoreIsCompoundFile));
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

    if (format <= Lucene3xSegmentInfoFormat.FORMAT_HAS_VECTORS) {
      // NOTE: unused
      final int hasVectors = input.readByte();
    }

    // Replicate logic from 3.x's SegmentInfo.files():
    final Set<String> files = new HashSet<String>();
    if (isCompoundFile) {
      files.add(IndexFileNames.segmentFileName(name, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
    } else {
      addIfExists(dir, files, IndexFileNames.segmentFileName(name, "", Lucene3xFieldInfosReader.FIELD_INFOS_EXTENSION));
      addIfExists(dir, files, IndexFileNames.segmentFileName(name, "", Lucene3xPostingsFormat.FREQ_EXTENSION));
      addIfExists(dir, files, IndexFileNames.segmentFileName(name, "", Lucene3xPostingsFormat.PROX_EXTENSION));
      addIfExists(dir, files, IndexFileNames.segmentFileName(name, "", Lucene3xPostingsFormat.TERMS_EXTENSION));
      addIfExists(dir, files, IndexFileNames.segmentFileName(name, "", Lucene3xPostingsFormat.TERMS_INDEX_EXTENSION));
      addIfExists(dir, files, IndexFileNames.segmentFileName(name, "", Lucene3xNormsProducer.NORMS_EXTENSION));
    }
    
    if (docStoreOffset != -1) {
      if (docStoreIsCompoundFile) {
        files.add(IndexFileNames.segmentFileName(docStoreSegment, "", Lucene3xCodec.COMPOUND_FILE_STORE_EXTENSION));
      } else {
        files.add(IndexFileNames.segmentFileName(docStoreSegment, "", Lucene3xStoredFieldsReader.FIELDS_INDEX_EXTENSION));
        files.add(IndexFileNames.segmentFileName(docStoreSegment, "", Lucene3xStoredFieldsReader.FIELDS_EXTENSION));
        addIfExists(dir, files, IndexFileNames.segmentFileName(docStoreSegment, "", Lucene3xTermVectorsReader.VECTORS_INDEX_EXTENSION));
        addIfExists(dir, files, IndexFileNames.segmentFileName(docStoreSegment, "", Lucene3xTermVectorsReader.VECTORS_FIELDS_EXTENSION));
        addIfExists(dir, files, IndexFileNames.segmentFileName(docStoreSegment, "", Lucene3xTermVectorsReader.VECTORS_DOCUMENTS_EXTENSION));
      }
    } else if (!isCompoundFile) {
      files.add(IndexFileNames.segmentFileName(name, "", Lucene3xStoredFieldsReader.FIELDS_INDEX_EXTENSION));
      files.add(IndexFileNames.segmentFileName(name, "", Lucene3xStoredFieldsReader.FIELDS_EXTENSION));
      addIfExists(dir, files, IndexFileNames.segmentFileName(name, "", Lucene3xTermVectorsReader.VECTORS_INDEX_EXTENSION));
      addIfExists(dir, files, IndexFileNames.segmentFileName(name, "", Lucene3xTermVectorsReader.VECTORS_FIELDS_EXTENSION));
      addIfExists(dir, files, IndexFileNames.segmentFileName(name, "", Lucene3xTermVectorsReader.VECTORS_DOCUMENTS_EXTENSION));
    }
    
    // parse the normgen stuff and shove it into attributes
    if (normGen != null) {
      attributes.put(Lucene3xSegmentInfoFormat.NORMGEN_KEY, Integer.toString(numNormGen));
      for(Map.Entry<Integer,Long> ent : normGen.entrySet()) {
        long gen = ent.getValue();
        if (gen >= SegmentInfo.YES) {
          // Definitely a separate norm file, with generation:
          files.add(IndexFileNames.fileNameFromGeneration(name, "s" + ent.getKey(), gen));
          attributes.put(Lucene3xSegmentInfoFormat.NORMGEN_PREFIX + ent.getKey(), Long.toString(gen));
        } else if (gen == SegmentInfo.NO) {
          // No separate norm
        } else {
          // We should have already hit indexformat too old exception
          assert false;
        }
      }
    }

    SegmentInfo info = new SegmentInfo(dir, version, name, docCount, isCompoundFile,
                                       null, diagnostics, Collections.unmodifiableMap(attributes));
    info.setFiles(files);

    SegmentInfoPerCommit infoPerCommit = new SegmentInfoPerCommit(info, delCount, delGen);
    return infoPerCommit;
  }

  private SegmentInfo readUpgradedSegmentInfo(String name, Directory dir, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene3xSegmentInfoFormat.UPGRADED_SI_CODEC_NAME,
                                 Lucene3xSegmentInfoFormat.UPGRADED_SI_VERSION_START,
                                 Lucene3xSegmentInfoFormat.UPGRADED_SI_VERSION_CURRENT);
    final String version = input.readString();

    final int docCount = input.readInt();
    
    final Map<String,String> attributes = input.readStringStringMap();

    final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;

    final Map<String,String> diagnostics = input.readStringStringMap();

    final Set<String> files = input.readStringSet();

    SegmentInfo info = new SegmentInfo(dir, version, name, docCount, isCompoundFile,
                                       null, diagnostics, Collections.unmodifiableMap(attributes));
    info.setFiles(files);
    return info;
  }
}
