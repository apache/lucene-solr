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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Constants;

/**
 * Information about a segment such as it's name, directory, and files related
 * to the segment.
 *
 * @lucene.experimental
 */
public final class SegmentInfo implements Cloneable {
  // TODO: remove with hasVector and hasProx
  public static final int CHECK_FIELDINFO = -2;
  
  // TODO: remove these from this class, for now this is the representation
  public static final int NO = -1;          // e.g. no norms; no deletes;
  public static final int YES = 1;          // e.g. have norms; have deletes;
  public static final int WITHOUT_GEN = 0;  // a file name that has no GEN in it.

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

  private volatile long sizeInBytes = -1;           // total byte size of all files (computed on demand)

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
  private int hasProx = CHECK_FIELDINFO;     // True if this segment has any fields with positional information

  
  private FieldInfos fieldInfos;

  private Codec codec;

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
                     Codec codec, FieldInfos fieldInfos) {
    this.name = name;
    this.docCount = docCount;
    this.dir = dir;
    delGen = NO;
    this.isCompoundFile = isCompoundFile;
    this.docStoreOffset = -1;
    this.docStoreSegment = name;
    this.codec = codec;
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
    fieldInfos = src.fieldInfos == null ? null : src.fieldInfos.clone();
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
    codec = src.codec;
  }

  void setDiagnostics(Map<String, String> diagnostics) {
    this.diagnostics = diagnostics;
  }

  public Map<String, String> getDiagnostics() {
    return diagnostics;
  }

  /**
   * Construct a new complete SegmentInfo instance from input.
   * <p>Note: this is public only to allow access from
   * the codecs package.</p>
   */
  public SegmentInfo(Directory dir, String version, String name, int docCount, long delGen, int docStoreOffset,
      String docStoreSegment, boolean docStoreIsCompoundFile, Map<Integer,Long> normGen, boolean isCompoundFile,
      int delCount, int hasProx, Codec codec, Map<String,String> diagnostics, int hasVectors) {
    this.dir = dir;
    this.version = version;
    this.name = name;
    this.docCount = docCount;
    this.delGen = delGen;
    this.docStoreOffset = docStoreOffset;
    this.docStoreSegment = docStoreSegment;
    this.docStoreIsCompoundFile = docStoreIsCompoundFile;
    this.normGen = normGen;
    this.isCompoundFile = isCompoundFile;
    this.delCount = delCount;
    this.hasProx = hasProx;
    this.codec = codec;
    this.diagnostics = diagnostics;
    this.hasVectors = hasVectors;
  }

  synchronized void loadFieldInfos(Directory dir, boolean checkCompoundFile) throws IOException {
    if (fieldInfos == null) {
      Directory dir0 = dir;
      if (isCompoundFile && checkCompoundFile) {
        dir0 = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(name,
            "", IndexFileNames.COMPOUND_FILE_EXTENSION), IOContext.READONCE, false);
      }
      try {
        FieldInfosReader reader = codec.fieldInfosFormat().getFieldInfosReader();
        fieldInfos = reader.read(dir0, name, IOContext.READONCE);
      } finally {
        if (dir != dir0) {
          dir0.close();
        }
      }
    }
  }

  /**
   * Returns total size in bytes of all of files used by this segment
   */
  public long sizeInBytes() throws IOException {
      long sum = 0;
      for (final String fileName : files()) {
        sum += dir.fileLength(fileName);
      }
      sizeInBytes = sum;
      return sizeInBytes;
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
  public SegmentInfo clone() {
    final SegmentInfo si = new SegmentInfo(name, docCount, dir, isCompoundFile, codec,
        fieldInfos == null ? null : fieldInfos.clone());
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

  /**
   * @deprecated separate norms are not supported in >= 4.0
   */
  @Deprecated
  boolean hasSeparateNorms() {
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

  public boolean getHasProx() throws IOException {
    return hasProx == CHECK_FIELDINFO ? getFieldInfos().hasProx() : hasProx == YES;
  }

  /** Can only be called once. */
  public void setCodec(Codec codec) {
    assert this.codec == null;
    if (codec == null) {
      throw new IllegalArgumentException("segmentCodecs must be non-null");
    }
    this.codec = codec;
  }

  public Codec getCodec() {
    return codec;
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

    codec.files(this, fileSet);

    files = new ArrayList<String>(fileSet);

    return files;
  }

  /* Called whenever any change is made that affects which
   * files this segment has. */
  private void clearFilesCache() {
    files = null;
    sizeInBytes = -1;
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
   *  '?' if it's unknown); it's using compound file
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
    } catch (Throwable e) {
      // Messy: because getHasVectors may be used in an
      // thread-unsafe way, and may attempt to open an fnm
      // file that has since (legitimately) been deleted by
      // IndexWriter, instead of throwing these exceptions
      // up, just add v? to indicate we don't know if this
      // segment has vectors:
      s.append("v?");
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
  
  /** @lucene.internal */
  public long getDelGen() {
    return delGen;
  }
  
  /** @lucene.internal */
  public Map<Integer,Long> getNormGen() {
    return normGen;
  }
  
  // TODO: clean up this SI/FI stuff here
  /** returns the 'real' value for hasProx (doesn't consult fieldinfos) 
   * @lucene.internal */
  public int getHasProxInternal() {
    return hasProx;
  }
  
  /** returns the 'real' value for hasVectors (doesn't consult fieldinfos) 
   * @lucene.internal */
  public int getHasVectorsInternal() {
    return hasVectors;
  }
}
