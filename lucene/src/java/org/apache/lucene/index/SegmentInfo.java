package org.apache.lucene.index;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.DefaultSegmentInfosWriter;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.StringHelper;

/**
 * Information about a segment such as it's name, directory, and files related
 * to the segment.
 *
 * @lucene.experimental
 */
public final class SegmentInfo implements Cloneable {
  // TODO: remove with hasVector and hasProx
  private static final int CHECK_FIELDINFO = -2;
  static final int NO = -1;          // e.g. no norms; no deletes;
  static final int YES = 1;          // e.g. have norms; have deletes;
  static final int WITHOUT_GEN = 0;  // a file name that has no GEN in it.

  public String name;				  // unique name in dir
  public int docCount;				  // number of docs in seg
  public Directory dir;				  // where segment resides

  /*
   * Current generation of del file:
   * - NO if there are no deletes
   * - YES or higher if there are deletes at generation N
   */
  private long delGen;

  /*
   * Current generation of each field's norm file. If this array is null,
   * means no separate norms. If this array is not null, its values mean:
   * - NO says this field has no separate norms
   * >= YES says this field has separate norms with the specified generation
   */
  private Map<Integer,Long> normGen;

  private boolean isCompoundFile;

  private volatile List<String> files;                     // cached list of files that this segment uses
                                                  // in the Directory

  private volatile long sizeInBytesNoStore = -1;           // total byte size of all but the store files (computed on demand)
  private volatile long sizeInBytesWithStore = -1;         // total byte size of all of our files (computed on demand)

  //TODO: LUCENE-2555: remove once we don't need to support shared doc stores (pre 4.0)
  private int docStoreOffset;                     // if this segment shares stored fields & vectors, this
                                                  // offset is where in that file this segment's docs begin
  //TODO: LUCENE-2555: remove once we don't need to support shared doc stores (pre 4.0)
  private String docStoreSegment;                 // name used to derive fields/vectors file we share with
                                                  // other segments
  //TODO: LUCENE-2555: remove once we don't need to support shared doc stores (pre 4.0)
  private boolean docStoreIsCompoundFile;         // whether doc store files are stored in compound file (*.cfx)

  private int delCount;                           // How many deleted docs in this segment
  
  //TODO: remove when we don't have to support old indexes anymore that had this field
  private int hasVectors = CHECK_FIELDINFO;
  //TODO: remove when we don't have to support old indexes anymore that had this field
  private int hasProx = CHECK_FIELDINFO;     // True if this segment has any fields with omitTermFreqAndPositions==false

  
  private FieldInfos fieldInfos;

  private SegmentCodecs segmentCodecs;

  private Map<String,String> diagnostics;

  // Tracks the Lucene version this segment was created with, since 3.1. Null
  // indicates an older than 3.0 index, and it's used to detect a too old index.
  // The format expected is "x.y" - "2.x" for pre-3.0 indexes (or null), and
  // specific versions afterwards ("3.0", "3.1" etc.).
  // see Constants.LUCENE_MAIN_VERSION.
  private String version;

  // NOTE: only used in-RAM by IW to track buffered deletes;
  // this is never written to/read from the Directory
  private long bufferedDeletesGen;
  
  // holds the fieldInfos Version to refresh files() cache if FI has changed
  private long fieldInfosVersion;
  
  public SegmentInfo(String name, int docCount, Directory dir, boolean isCompoundFile,
                     SegmentCodecs segmentCodecs, FieldInfos fieldInfos) {
    this.name = name;
    this.docCount = docCount;
    this.dir = dir;
    delGen = NO;
    this.isCompoundFile = isCompoundFile;
    this.docStoreOffset = -1;
    this.docStoreSegment = name;
    this.segmentCodecs = segmentCodecs;
    delCount = 0;
    version = Constants.LUCENE_MAIN_VERSION;
    this.fieldInfos = fieldInfos;
  }

  /**
   * Copy everything from src SegmentInfo into our instance.
   */
  void reset(SegmentInfo src) {
    clearFilesCache();
    version = src.version;
    name = src.name;
    docCount = src.docCount;
    dir = src.dir;
    delGen = src.delGen;
    docStoreOffset = src.docStoreOffset;
    docStoreSegment = src.docStoreSegment;
    docStoreIsCompoundFile = src.docStoreIsCompoundFile;
    hasVectors = src.hasVectors;
    hasProx = src.hasProx;
    fieldInfos = src.fieldInfos == null ? null : (FieldInfos) src.fieldInfos.clone();
    if (src.normGen == null) {
      normGen = null;
    } else {
      normGen = new HashMap<Integer, Long>(src.normGen.size());
      for (Entry<Integer,Long> entry : src.normGen.entrySet()) {
        normGen.put(entry.getKey(), entry.getValue());
      }
    }
    isCompoundFile = src.isCompoundFile;
    delCount = src.delCount;
    segmentCodecs = src.segmentCodecs;
  }

  void setDiagnostics(Map<String, String> diagnostics) {
    this.diagnostics = diagnostics;
  }

  public Map<String, String> getDiagnostics() {
    return diagnostics;
  }

  /**
   * Construct a new SegmentInfo instance by reading a
   * previously saved SegmentInfo from input.
   * <p>Note: this is public only to allow access from
   * the codecs package.</p>
   *
   * @param dir directory to load from
   * @param format format of the segments info file
   * @param input input handle to read segment info from
   */
  public SegmentInfo(Directory dir, int format, IndexInput input, CodecProvider codecs) throws IOException {
    this.dir = dir;
    if (format <= DefaultSegmentInfosWriter.FORMAT_3_1) {
      version = input.readString();
    }
    name = input.readString();
    docCount = input.readInt();
    delGen = input.readLong();
    docStoreOffset = input.readInt();
    if (docStoreOffset != -1) {
      docStoreSegment = input.readString();
      docStoreIsCompoundFile = input.readByte() == YES;
    } else {
      docStoreSegment = name;
      docStoreIsCompoundFile = false;
    }

    if (format > DefaultSegmentInfosWriter.FORMAT_4_0) {
      // pre-4.0 indexes write a byte if there is a single norms file
      byte b = input.readByte();
      assert 1 == b;
    }

    int numNormGen = input.readInt();
    if (numNormGen == NO) {
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
    isCompoundFile = input.readByte() == YES;

    delCount = input.readInt();
    assert delCount <= docCount;

    hasProx = input.readByte();

    // System.out.println(Thread.currentThread().getName() + ": si.read hasProx=" + hasProx + " seg=" + name);
    if (format <= DefaultSegmentInfosWriter.FORMAT_4_0) {
      segmentCodecs = new SegmentCodecs(codecs, input);
    } else {
      // codec ID on FieldInfo is 0 so it will simply use the first codec available
      // TODO what todo if preflex is not available in the provider? register it or fail?
      segmentCodecs = new SegmentCodecs(codecs, new Codec[] { codecs.lookup("PreFlex")});
    }
    diagnostics = input.readStringStringMap();

    if (format <= DefaultSegmentInfosWriter.FORMAT_HAS_VECTORS) {
      hasVectors = input.readByte();
    } else {
      final String storesSegment;
      final String ext;
      final boolean isCompoundFile;
      if (docStoreOffset != -1) {
        storesSegment = docStoreSegment;
        isCompoundFile = docStoreIsCompoundFile;
        ext = IndexFileNames.COMPOUND_FILE_STORE_EXTENSION;
      } else {
        storesSegment = name;
        isCompoundFile = getUseCompoundFile();
        ext = IndexFileNames.COMPOUND_FILE_EXTENSION;
      }
      final Directory dirToTest;
      if (isCompoundFile) {
        dirToTest = dir.openCompoundInput(IndexFileNames.segmentFileName(storesSegment, "", ext), BufferedIndexInput.BUFFER_SIZE);
      } else {
        dirToTest = dir;
      }
      try {
        hasVectors = dirToTest.fileExists(IndexFileNames.segmentFileName(storesSegment, "", IndexFileNames.VECTORS_INDEX_EXTENSION)) ? YES : NO;
      } finally {
        if (isCompoundFile) {
          dirToTest.close();
        }
      }
    }
  }
  
  synchronized void loadFieldInfos(Directory dir, boolean checkCompoundFile) throws IOException {
    if (fieldInfos == null) {
      Directory dir0 = dir;
      if (isCompoundFile && checkCompoundFile) {
        dir0 = dir.openCompoundInput(IndexFileNames.segmentFileName(name,
            "", IndexFileNames.COMPOUND_FILE_EXTENSION), BufferedIndexInput.BUFFER_SIZE);
      }
      try {
        fieldInfos = new FieldInfos(dir0, IndexFileNames.segmentFileName(name,
            "", IndexFileNames.FIELD_INFOS_EXTENSION));
      } finally {
        if (dir != dir0) {
          dir0.close();
        }
      }
    }
  }

  /**
   * Returns total size in bytes of all of files used by this segment (if
   * {@code includeDocStores} is true), or the size of all files except the
   * store files otherwise.
   */
  public long sizeInBytes(boolean includeDocStores) throws IOException {
    if (includeDocStores) {
      if (sizeInBytesWithStore != -1) {
        return sizeInBytesWithStore;
      }
      long sum = 0;
      for (final String fileName : files()) {
        // We don't count bytes used by a shared doc store
        // against this segment
        if (docStoreOffset == -1 || !IndexFileNames.isDocStoreFile(fileName)) {
          sum += dir.fileLength(fileName);
        }
      }
      sizeInBytesWithStore = sum;
      return sizeInBytesWithStore;
    } else {
      if (sizeInBytesNoStore != -1) {
        return sizeInBytesNoStore;
      }
      long sum = 0;
      for (final String fileName : files()) {
        if (IndexFileNames.isDocStoreFile(fileName)) {
          continue;
        }
        sum += dir.fileLength(fileName);
      }
      sizeInBytesNoStore = sum;
      return sizeInBytesNoStore;
    }
  }

  public boolean getHasVectors() throws IOException {
    return hasVectors == CHECK_FIELDINFO ? getFieldInfos().hasVectors() : hasVectors == YES;
  }
  
  public FieldInfos getFieldInfos() throws IOException {
    loadFieldInfos(dir, true);
    return fieldInfos;
  }

  public boolean hasDeletions() {
    // Cases:
    //
    //   delGen == NO: this means this segment does not have deletions yet
    //   delGen >= YES: this means this segment has deletions
    //
    return delGen != NO;
  }

  void advanceDelGen() {
    if (delGen == NO) {
      delGen = YES;
    } else {
      delGen++;
    }
    clearFilesCache();
  }

  void clearDelGen() {
    delGen = NO;
    clearFilesCache();
  }

  @Override
  public Object clone() {
    final SegmentInfo si = new SegmentInfo(name, docCount, dir, isCompoundFile, segmentCodecs,
        fieldInfos == null ? null : (FieldInfos) fieldInfos.clone());
    si.docStoreOffset = docStoreOffset;
    si.docStoreSegment = docStoreSegment;
    si.docStoreIsCompoundFile = docStoreIsCompoundFile;
    si.delGen = delGen;
    si.delCount = delCount;
    si.diagnostics = new HashMap<String, String>(diagnostics);
    if (normGen != null) {
      si.normGen = new HashMap<Integer, Long>();
      for (Entry<Integer,Long> entry : normGen.entrySet()) {
        si.normGen.put(entry.getKey(), entry.getValue());
      }
    }
    si.version = version;
    si.hasProx = hasProx;
    si.hasVectors = hasVectors;
    return si;
  }

  public String getDelFileName() {
    if (delGen == NO) {
      // In this case we know there is no deletion filename
      // against this segment
      return null;
    } else {
      return IndexFileNames.fileNameFromGeneration(name, IndexFileNames.DELETES_EXTENSION, delGen);
    }
  }

  /**
   * Returns true if this field for this segment has saved a separate norms file (_<segment>_N.sX).
   *
   * @param fieldNumber the field index to check
   */
  public boolean hasSeparateNorms(int fieldNumber) {
    if (normGen == null) {
      return false;
    }

    Long gen = normGen.get(fieldNumber);
    return gen != null && gen.longValue() != NO;
  }

  /**
   * Returns true if any fields in this segment have separate norms.
   */
  public boolean hasSeparateNorms() {
    if (normGen == null) {
      return false;
    } else {
      for (long fieldNormGen : normGen.values()) {
        if (fieldNormGen >= YES) {
          return true;
        }
      }
    }

    return false;
  }

  void initNormGen() {
    if (normGen == null) { // normGen is null if this segments file hasn't had any norms set against it yet
      normGen = new HashMap<Integer, Long>();
    }
  }

  /**
   * Increment the generation count for the norms file for
   * this field.
   *
   * @param fieldIndex field whose norm file will be rewritten
   */
  void advanceNormGen(int fieldIndex) {
    Long gen = normGen.get(fieldIndex);
    if (gen == null || gen.longValue() == NO) {
      normGen.put(fieldIndex, new Long(YES));
    } else {
      normGen.put(fieldIndex, gen+1);
    }
    clearFilesCache();
  }

  /**
   * Get the file name for the norms file for this field.
   *
   * @param number field index
   */
  public String getNormFileName(int number) {
    if (hasSeparateNorms(number)) {
      return IndexFileNames.fileNameFromGeneration(name, IndexFileNames.SEPARATE_NORMS_EXTENSION + number, normGen.get(number));
    } else {
      // single file for all norms
      return IndexFileNames.fileNameFromGeneration(name, IndexFileNames.NORMS_EXTENSION, WITHOUT_GEN);
    }
  }

  /**
   * Mark whether this segment is stored as a compound file.
   *
   * @param isCompoundFile true if this is a compound file;
   * else, false
   */
  void setUseCompoundFile(boolean isCompoundFile) {
    this.isCompoundFile = isCompoundFile;
    clearFilesCache();
  }
  
  /**
   * Returns true if this segment is stored as a compound
   * file; else, false.
   */
  public boolean getUseCompoundFile() {
    return isCompoundFile;
  }

  public int getDelCount() {
    return delCount;
  }

  void setDelCount(int delCount) {
    this.delCount = delCount;
    assert delCount <= docCount;
  }

  /**
   * @deprecated shared doc stores are not supported in >= 4.0
   */
  @Deprecated
  public int getDocStoreOffset() {
    // TODO: LUCENE-2555: remove once we don't need to support shared doc stores (pre 4.0)
    return docStoreOffset;
  }

  /**
   * @deprecated shared doc stores are not supported in >= 4.0
   */
  @Deprecated
  public boolean getDocStoreIsCompoundFile() {
    // TODO: LUCENE-2555: remove once we don't need to support shared doc stores (pre 4.0)
    return docStoreIsCompoundFile;
  }

  /**
   * @deprecated shared doc stores are not supported in >= 4.0
   */
  @Deprecated
  public void setDocStoreIsCompoundFile(boolean docStoreIsCompoundFile) {
    // TODO: LUCENE-2555: remove once we don't need to support shared doc stores (pre 4.0)
    this.docStoreIsCompoundFile = docStoreIsCompoundFile;
    clearFilesCache();
  }

  /**
   * @deprecated shared doc stores are not supported in >= 4.0
   */
  @Deprecated
  void setDocStore(int offset, String segment, boolean isCompoundFile) {
    // TODO: LUCENE-2555: remove once we don't need to support shared doc stores (pre 4.0)
    docStoreOffset = offset;
    docStoreSegment = segment;
    docStoreIsCompoundFile = isCompoundFile;
    clearFilesCache();
  }

  /**
   * @deprecated shared doc stores are not supported in >= 4.0
   */
  @Deprecated
  public String getDocStoreSegment() {
    // TODO: LUCENE-2555: remove once we don't need to support shared doc stores (pre 4.0)
    return docStoreSegment;
  }

  /**
   * @deprecated shared doc stores are not supported in >= 4.0
   */
  @Deprecated
  void setDocStoreOffset(int offset) {
    // TODO: LUCENE-2555: remove once we don't need to support shared doc stores (pre 4.0)
    docStoreOffset = offset;
    clearFilesCache();
  }

  /**
   * @deprecated shared doc stores are not supported in 4.0
   */
  @Deprecated
  public void setDocStoreSegment(String docStoreSegment) {
    // TODO: LUCENE-2555: remove once we don't need to support shared doc stores (pre 4.0)
    this.docStoreSegment = docStoreSegment;
  }

  /** Save this segment's info. */
  public void write(IndexOutput output)
    throws IOException {
    assert delCount <= docCount: "delCount=" + delCount + " docCount=" + docCount + " segment=" + name;
    // Write the Lucene version that created this segment, since 3.1
    output.writeString(version);
    output.writeString(name);
    output.writeInt(docCount);
    output.writeLong(delGen);

    output.writeInt(docStoreOffset);
    if (docStoreOffset != -1) {
      output.writeString(docStoreSegment);
      output.writeByte((byte) (docStoreIsCompoundFile ? 1:0));
    }


    if (normGen == null) {
      output.writeInt(NO);
    } else {
      output.writeInt(normGen.size());
      for (Entry<Integer,Long> entry : normGen.entrySet()) {
        output.writeInt(entry.getKey());
        output.writeLong(entry.getValue());
      }
    }

    output.writeByte((byte) (isCompoundFile ? YES : NO));
    output.writeInt(delCount);
    output.writeByte((byte) (hasProx));
    segmentCodecs.write(output);
    output.writeStringStringMap(diagnostics);
    output.writeByte((byte) (hasVectors));
  }

  public boolean getHasProx() throws IOException {
    return hasProx == CHECK_FIELDINFO ? getFieldInfos().hasProx() : hasProx == YES;
  }

  /** Can only be called once. */
  public void setSegmentCodecs(SegmentCodecs segmentCodecs) {
    assert this.segmentCodecs == null;
    if (segmentCodecs == null) {
      throw new IllegalArgumentException("segmentCodecs must be non-null");
    }
    this.segmentCodecs = segmentCodecs;
  }

  SegmentCodecs getSegmentCodecs() {
    return segmentCodecs;
  }

  private void addIfExists(Set<String> files, String fileName) throws IOException {
    if (dir.fileExists(fileName))
      files.add(fileName);
  }

  /*
   * Return all files referenced by this SegmentInfo.  The
   * returns List is a locally cached List so you should not
   * modify it.
   */

  public List<String> files() throws IOException {
    final long fisVersion = fieldInfosVersion;
    if (fisVersion != (fieldInfosVersion = getFieldInfos().getVersion())) {
      clearFilesCache(); // FIS has modifications - need to recompute
    } else if (files != null) {
      // Already cached:
      return files;
    }
    final Set<String> fileSet = new HashSet<String>();

    boolean useCompoundFile = getUseCompoundFile();

    if (useCompoundFile) {
      fileSet.add(IndexFileNames.segmentFileName(name, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
      if (version != null && StringHelper.getVersionComparator().compare("3.4", version) <= 0) {
        fileSet.add(IndexFileNames.segmentFileName(name, "",
            IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
      }
    } else {
      for(String ext : IndexFileNames.NON_STORE_INDEX_EXTENSIONS) {
        addIfExists(fileSet, IndexFileNames.segmentFileName(name, "", ext));
      }
      segmentCodecs.files(dir, this, fileSet);
    }

    if (docStoreOffset != -1) {
      // We are sharing doc stores (stored fields, term
      // vectors) with other segments
      assert docStoreSegment != null;
      if (docStoreIsCompoundFile) {
        fileSet.add(IndexFileNames.segmentFileName(docStoreSegment, "", IndexFileNames.COMPOUND_FILE_STORE_EXTENSION));
      } else {
        fileSet.add(IndexFileNames.segmentFileName(docStoreSegment, "", IndexFileNames.FIELDS_INDEX_EXTENSION));
        fileSet.add(IndexFileNames.segmentFileName(docStoreSegment, "", IndexFileNames.FIELDS_EXTENSION));
        if (getHasVectors()) {
          fileSet.add(IndexFileNames.segmentFileName(docStoreSegment, "", IndexFileNames.VECTORS_INDEX_EXTENSION));
          fileSet.add(IndexFileNames.segmentFileName(docStoreSegment, "", IndexFileNames.VECTORS_DOCUMENTS_EXTENSION));
          fileSet.add(IndexFileNames.segmentFileName(docStoreSegment, "", IndexFileNames.VECTORS_FIELDS_EXTENSION));
        }
      }
    } else if (!useCompoundFile) {
      fileSet.add(IndexFileNames.segmentFileName(name, "", IndexFileNames.FIELDS_INDEX_EXTENSION));
      fileSet.add(IndexFileNames.segmentFileName(name, "", IndexFileNames.FIELDS_EXTENSION));
      if (getHasVectors()) {
        fileSet.add(IndexFileNames.segmentFileName(name, "", IndexFileNames.VECTORS_INDEX_EXTENSION));
        fileSet.add(IndexFileNames.segmentFileName(name, "", IndexFileNames.VECTORS_DOCUMENTS_EXTENSION));
        fileSet.add(IndexFileNames.segmentFileName(name, "", IndexFileNames.VECTORS_FIELDS_EXTENSION));
      }
    }

    String delFileName = IndexFileNames.fileNameFromGeneration(name, IndexFileNames.DELETES_EXTENSION, delGen);
    if (delFileName != null && (delGen >= YES || dir.fileExists(delFileName))) {
      fileSet.add(delFileName);
    }
   
    if (normGen != null) {
      for (Entry<Integer,Long> entry : normGen.entrySet()) {
        long gen = entry.getValue();
        if (gen >= YES) {
          // Definitely a separate norm file, with generation:
          fileSet.add(IndexFileNames.fileNameFromGeneration(name, IndexFileNames.SEPARATE_NORMS_EXTENSION + entry.getKey(), gen));
        }
      }
    }

    files = new ArrayList<String>(fileSet);

    return files;
  }

  /* Called whenever any change is made that affects which
   * files this segment has. */
  private void clearFilesCache() {
    files = null;
    sizeInBytesNoStore = -1;
    sizeInBytesWithStore = -1;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return toString(dir, 0);
  }

  /** Used for debugging.  Format may suddenly change.
   *
   *  <p>Current format looks like
   *  <code>_a(3.1):c45/4->_1</code>, which means the segment's
   *  name is <code>_a</code>; it was created with Lucene 3.1 (or
   *  '?' if it's unkown); it's using compound file
   *  format (would be <code>C</code> if not compound); it
   *  has 45 documents; it has 4 deletions (this part is
   *  left off when there are no deletions); it's using the
   *  shared doc stores named <code>_1</code> (this part is
   *  left off if doc stores are private).</p>
   */
  public String toString(Directory dir, int pendingDelCount) {

    StringBuilder s = new StringBuilder();
    s.append(name).append('(').append(version == null ? "?" : version).append(')').append(':');
    char cfs = getUseCompoundFile() ? 'c' : 'C';
    s.append(cfs);

    if (this.dir != dir) {
      s.append('x');
    }
    try {
      if (getHasVectors()) {
        s.append('v');
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    s.append(docCount);

    int delCount = getDelCount() + pendingDelCount;
    if (delCount != 0) {
      s.append('/').append(delCount);
    }

    if (docStoreOffset != -1) {
      s.append("->").append(docStoreSegment);
      if (docStoreIsCompoundFile) {
        s.append('c');
      } else {
        s.append('C');
      }
      s.append('+').append(docStoreOffset);
    }

    return s.toString();
  }

  /** We consider another SegmentInfo instance equal if it
   *  has the same dir and same name. */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj instanceof SegmentInfo) {
      final SegmentInfo other = (SegmentInfo) obj;
      return other.dir == dir && other.name.equals(name);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return dir.hashCode() + name.hashCode();
  }

  /**
   * Used by DefaultSegmentInfosReader to upgrade a 3.0 segment to record its
   * version is "3.0". This method can be removed when we're not required to
   * support 3x indexes anymore, e.g. in 5.0.
   * <p>
   * <b>NOTE:</b> this method is used for internal purposes only - you should
   * not modify the version of a SegmentInfo, or it may result in unexpected
   * exceptions thrown when you attempt to open the index.
   *
   * @lucene.internal
   */
  public void setVersion(String version) {
    this.version = version;
  }

  /** Returns the version of the code which wrote the segment. */
  public String getVersion() {
    return version;
  }

  long getBufferedDeletesGen() {
    return bufferedDeletesGen;
  }

  void setBufferedDeletesGen(long v) {
    bufferedDeletesGen = v;
  }
}
