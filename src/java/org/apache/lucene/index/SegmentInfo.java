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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitVector;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Information about a segment such as it's name, directory, and files related
 * to the segment.
 * 
 * * <p><b>NOTE:</b> This API is new and still experimental
 * (subject to change suddenly in the next release)</p>
 */
public final class SegmentInfo {

  static final int NO = -1;          // e.g. no norms; no deletes;
  static final int YES = 1;          // e.g. have norms; have deletes;
  static final int CHECK_DIR = 0;    // e.g. must check dir to see if there are norms/deletions
  static final int WITHOUT_GEN = 0;  // a file name that has no GEN in it. 

  public String name;				  // unique name in dir
  public int docCount;				  // number of docs in seg
  public Directory dir;				  // where segment resides

  private boolean preLockless;                    // true if this is a segments file written before
                                                  // lock-less commits (2.1)

  private long delGen;                            // current generation of del file; NO if there
                                                  // are no deletes; CHECK_DIR if it's a pre-2.1 segment
                                                  // (and we must check filesystem); YES or higher if
                                                  // there are deletes at generation N
   
  private long[] normGen;                         // current generation of each field's norm file.
                                                  // If this array is null, for lockLess this means no 
                                                  // separate norms.  For preLockLess this means we must 
                                                  // check filesystem. If this array is not null, its 
                                                  // values mean: NO says this field has no separate  
                                                  // norms; CHECK_DIR says it is a preLockLess segment and    
                                                  // filesystem must be checked; >= YES says this field  
                                                  // has separate norms with the specified generation

  private byte isCompoundFile;                    // NO if it is not; YES if it is; CHECK_DIR if it's
                                                  // pre-2.1 (ie, must check file system to see
                                                  // if <name>.cfs and <name>.nrm exist)         

  private boolean hasSingleNormFile;              // true if this segment maintains norms in a single file; 
                                                  // false otherwise
                                                  // this is currently false for segments populated by DocumentWriter
                                                  // and true for newly created merged segments (both
                                                  // compound and non compound).
  
  private List files;                             // cached list of files that this segment uses
                                                  // in the Directory

  long sizeInBytes = -1;                          // total byte size of all of our files (computed on demand)

  private int docStoreOffset;                     // if this segment shares stored fields & vectors, this
                                                  // offset is where in that file this segment's docs begin
  private String docStoreSegment;                 // name used to derive fields/vectors file we share with
                                                  // other segments
  private boolean docStoreIsCompoundFile;         // whether doc store files are stored in compound file (*.cfx)

  private int delCount;                           // How many deleted docs in this segment, or -1 if not yet known
                                                  // (if it's an older index)

  private boolean hasProx;                        // True if this segment has any fields with omitTermFreqAndPositions==false

  private Map diagnostics;

  public String toString() {
    return "si: "+dir.toString()+" "+name+" docCount: "+docCount+" delCount: "+delCount+" delFileName: "+getDelFileName();
  }
  
  public SegmentInfo(String name, int docCount, Directory dir) {
    this.name = name;
    this.docCount = docCount;
    this.dir = dir;
    delGen = NO;
    isCompoundFile = CHECK_DIR;
    preLockless = true;
    hasSingleNormFile = false;
    docStoreOffset = -1;
    docStoreSegment = name;
    docStoreIsCompoundFile = false;
    delCount = 0;
    hasProx = true;
  }

  public SegmentInfo(String name, int docCount, Directory dir, boolean isCompoundFile, boolean hasSingleNormFile) { 
    this(name, docCount, dir, isCompoundFile, hasSingleNormFile, -1, null, false, true);
  }

  public SegmentInfo(String name, int docCount, Directory dir, boolean isCompoundFile, boolean hasSingleNormFile,
                     int docStoreOffset, String docStoreSegment, boolean docStoreIsCompoundFile, boolean hasProx) { 
    this(name, docCount, dir);
    this.isCompoundFile = (byte) (isCompoundFile ? YES : NO);
    this.hasSingleNormFile = hasSingleNormFile;
    preLockless = false;
    this.docStoreOffset = docStoreOffset;
    this.docStoreSegment = docStoreSegment;
    this.docStoreIsCompoundFile = docStoreIsCompoundFile;
    this.hasProx = hasProx;
    delCount = 0;
    assert docStoreOffset == -1 || docStoreSegment != null: "dso=" + docStoreOffset + " dss=" + docStoreSegment + " docCount=" + docCount;
  }

  /**
   * Copy everything from src SegmentInfo into our instance.
   */
  void reset(SegmentInfo src) {
    clearFiles();
    name = src.name;
    docCount = src.docCount;
    dir = src.dir;
    preLockless = src.preLockless;
    delGen = src.delGen;
    docStoreOffset = src.docStoreOffset;
    docStoreIsCompoundFile = src.docStoreIsCompoundFile;
    if (src.normGen == null) {
      normGen = null;
    } else {
      normGen = new long[src.normGen.length];
      System.arraycopy(src.normGen, 0, normGen, 0, src.normGen.length);
    }
    isCompoundFile = src.isCompoundFile;
    hasSingleNormFile = src.hasSingleNormFile;
    delCount = src.delCount;
  }

  // must be Map<String, String>
  void setDiagnostics(Map diagnostics) {
    this.diagnostics = diagnostics;
  }

  // returns Map<String, String>
  public Map getDiagnostics() {
    return diagnostics;
  }

  /**
   * Construct a new SegmentInfo instance by reading a
   * previously saved SegmentInfo from input.
   *
   * @param dir directory to load from
   * @param format format of the segments info file
   * @param input input handle to read segment info from
   */
  SegmentInfo(Directory dir, int format, IndexInput input) throws IOException {
    this.dir = dir;
    name = input.readString();
    docCount = input.readInt();
    if (format <= SegmentInfos.FORMAT_LOCKLESS) {
      delGen = input.readLong();
      if (format <= SegmentInfos.FORMAT_SHARED_DOC_STORE) {
        docStoreOffset = input.readInt();
        if (docStoreOffset != -1) {
          docStoreSegment = input.readString();
          docStoreIsCompoundFile = (1 == input.readByte());
        } else {
          docStoreSegment = name;
          docStoreIsCompoundFile = false;
        }
      } else {
        docStoreOffset = -1;
        docStoreSegment = name;
        docStoreIsCompoundFile = false;
      }
      if (format <= SegmentInfos.FORMAT_SINGLE_NORM_FILE) {
        hasSingleNormFile = (1 == input.readByte());
      } else {
        hasSingleNormFile = false;
      }
      int numNormGen = input.readInt();
      if (numNormGen == NO) {
        normGen = null;
      } else {
        normGen = new long[numNormGen];
        for(int j=0;j<numNormGen;j++) {
          normGen[j] = input.readLong();
        }
      }
      isCompoundFile = input.readByte();
      preLockless = (isCompoundFile == CHECK_DIR);
      if (format <= SegmentInfos.FORMAT_DEL_COUNT) {
        delCount = input.readInt();
        assert delCount <= docCount;
      } else
        delCount = -1;
      if (format <= SegmentInfos.FORMAT_HAS_PROX)
        hasProx = input.readByte() == 1;
      else
        hasProx = true;

      if (format <= SegmentInfos.FORMAT_DIAGNOSTICS) {
        diagnostics = input.readStringStringMap();
      } else {
        diagnostics = Collections.EMPTY_MAP;
      }
    } else {
      delGen = CHECK_DIR;
      normGen = null;
      isCompoundFile = CHECK_DIR;
      preLockless = true;
      hasSingleNormFile = false;
      docStoreOffset = -1;
      docStoreIsCompoundFile = false;
      docStoreSegment = null;
      delCount = -1;
      hasProx = true;
      diagnostics = Collections.EMPTY_MAP;
    }
  }
  
  void setNumFields(int numFields) {
    if (normGen == null) {
      // normGen is null if we loaded a pre-2.1 segment
      // file, or, if this segments file hasn't had any
      // norms set against it yet:
      normGen = new long[numFields];

      if (preLockless) {
        // Do nothing: thus leaving normGen[k]==CHECK_DIR (==0), so that later we know  
        // we have to check filesystem for norm files, because this is prelockless.
        
      } else {
        // This is a FORMAT_LOCKLESS segment, which means
        // there are no separate norms:
        for(int i=0;i<numFields;i++) {
          normGen[i] = NO;
        }
      }
    }
  }

  /** Returns total size in bytes of all of files used by
   *  this segment. */
  public long sizeInBytes() throws IOException {
    if (sizeInBytes == -1) {
      List files = files();
      final int size = files.size();
      sizeInBytes = 0;
      for(int i=0;i<size;i++) {
        final String fileName = (String) files.get(i);
        // We don't count bytes used by a shared doc store
        // against this segment:
        if (docStoreOffset == -1 || !IndexFileNames.isDocStoreFile(fileName))
          sizeInBytes += dir.fileLength(fileName);
      }
    }
    return sizeInBytes;
  }

  public boolean hasDeletions()
    throws IOException {
    // Cases:
    //
    //   delGen == NO: this means this segment was written
    //     by the LOCKLESS code and for certain does not have
    //     deletions yet
    //
    //   delGen == CHECK_DIR: this means this segment was written by
    //     pre-LOCKLESS code which means we must check
    //     directory to see if .del file exists
    //
    //   delGen >= YES: this means this segment was written by
    //     the LOCKLESS code and for certain has
    //     deletions
    //
    if (delGen == NO) {
      return false;
    } else if (delGen >= YES) {
      return true;
    } else {
      return dir.fileExists(getDelFileName());
    }
  }

  void advanceDelGen() {
    // delGen 0 is reserved for pre-LOCKLESS format
    if (delGen == NO) {
      delGen = YES;
    } else {
      delGen++;
    }
    clearFiles();
  }

  void clearDelGen() {
    delGen = NO;
    clearFiles();
  }

  public Object clone () {
    SegmentInfo si = new SegmentInfo(name, docCount, dir);
    si.isCompoundFile = isCompoundFile;
    si.delGen = delGen;
    si.delCount = delCount;
    si.hasProx = hasProx;
    si.preLockless = preLockless;
    si.hasSingleNormFile = hasSingleNormFile;
    si.diagnostics = new HashMap(diagnostics);
    if (normGen != null) {
      si.normGen = (long[]) normGen.clone();
    }
    si.docStoreOffset = docStoreOffset;
    si.docStoreSegment = docStoreSegment;
    si.docStoreIsCompoundFile = docStoreIsCompoundFile;
    return si;
  }

  public String getDelFileName() {
    if (delGen == NO) {
      // In this case we know there is no deletion filename
      // against this segment
      return null;
    } else {
      // If delGen is CHECK_DIR, it's the pre-lockless-commit file format
      return IndexFileNames.fileNameFromGeneration(name, "." + IndexFileNames.DELETES_EXTENSION, delGen); 
    }
  }

  /**
   * Returns true if this field for this segment has saved a separate norms file (_<segment>_N.sX).
   *
   * @param fieldNumber the field index to check
   */
  public boolean hasSeparateNorms(int fieldNumber)
    throws IOException {
    if ((normGen == null && preLockless) || (normGen != null && normGen[fieldNumber] == CHECK_DIR)) {
      // Must fallback to directory file exists check:
      String fileName = name + ".s" + fieldNumber;
      return dir.fileExists(fileName);
    } else if (normGen == null || normGen[fieldNumber] == NO) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Returns true if any fields in this segment have separate norms.
   */
  public boolean hasSeparateNorms()
    throws IOException {
    if (normGen == null) {
      if (!preLockless) {
        // This means we were created w/ LOCKLESS code and no
        // norms are written yet:
        return false;
      } else {
        // This means this segment was saved with pre-LOCKLESS
        // code.  So we must fallback to the original
        // directory list check:
        String[] result = dir.list();
        if (result == null)
          throw new IOException("cannot read directory " + dir + ": list() returned null");
        
        String pattern;
        pattern = name + ".s";
        int patternLength = pattern.length();
        for(int i = 0; i < result.length; i++){
          if(result[i].startsWith(pattern) && Character.isDigit(result[i].charAt(patternLength)))
            return true;
        }
        return false;
      }
    } else {
      // This means this segment was saved with LOCKLESS
      // code so we first check whether any normGen's are >= 1
      // (meaning they definitely have separate norms):
      for(int i=0;i<normGen.length;i++) {
        if (normGen[i] >= YES) {
          return true;
        }
      }
      // Next we look for any == 0.  These cases were
      // pre-LOCKLESS and must be checked in directory:
      for(int i=0;i<normGen.length;i++) {
        if (normGen[i] == CHECK_DIR) {
          if (hasSeparateNorms(i)) {
            return true;
          }
        }
      }
    }

    return false;
  }

  /**
   * Increment the generation count for the norms file for
   * this field.
   *
   * @param fieldIndex field whose norm file will be rewritten
   */
  void advanceNormGen(int fieldIndex) {
    if (normGen[fieldIndex] == NO) {
      normGen[fieldIndex] = YES;
    } else {
      normGen[fieldIndex]++;
    }
    clearFiles();
  }

  /**
   * Get the file name for the norms file for this field.
   *
   * @param number field index
   */
  public String getNormFileName(int number) throws IOException {
    String prefix;

    long gen;
    if (normGen == null) {
      gen = CHECK_DIR;
    } else {
      gen = normGen[number];
    }
    
    if (hasSeparateNorms(number)) {
      // case 1: separate norm
      prefix = ".s";
      return IndexFileNames.fileNameFromGeneration(name, prefix + number, gen);
    }

    if (hasSingleNormFile) {
      // case 2: lockless (or nrm file exists) - single file for all norms 
      prefix = "." + IndexFileNames.NORMS_EXTENSION;
      return IndexFileNames.fileNameFromGeneration(name, prefix, WITHOUT_GEN);
    }
      
    // case 3: norm file for each field
    prefix = ".f";
    return IndexFileNames.fileNameFromGeneration(name, prefix + number, WITHOUT_GEN);
  }

  /**
   * Mark whether this segment is stored as a compound file.
   *
   * @param isCompoundFile true if this is a compound file;
   * else, false
   */
  void setUseCompoundFile(boolean isCompoundFile) {
    if (isCompoundFile) {
      this.isCompoundFile = YES;
    } else {
      this.isCompoundFile = NO;
    }
    clearFiles();
  }

  /**
   * Returns true if this segment is stored as a compound
   * file; else, false.
   */
  public boolean getUseCompoundFile() throws IOException {
    if (isCompoundFile == NO) {
      return false;
    } else if (isCompoundFile == YES) {
      return true;
    } else {
      return dir.fileExists(name + "." + IndexFileNames.COMPOUND_FILE_EXTENSION);
    }
  }

  public int getDelCount() throws IOException {
    if (delCount == -1) {
      if (hasDeletions()) {
        final String delFileName = getDelFileName();
        delCount = new BitVector(dir, delFileName).count();
      } else
        delCount = 0;
    }
    assert delCount <= docCount;
    return delCount;
  }

  void setDelCount(int delCount) {
    this.delCount = delCount;
    assert delCount <= docCount;
  }

  public int getDocStoreOffset() {
    return docStoreOffset;
  }
  
  public boolean getDocStoreIsCompoundFile() {
    return docStoreIsCompoundFile;
  }
  
  void setDocStoreIsCompoundFile(boolean v) {
    docStoreIsCompoundFile = v;
    clearFiles();
  }
  
  public String getDocStoreSegment() {
    return docStoreSegment;
  }
  
  void setDocStoreOffset(int offset) {
    docStoreOffset = offset;
    clearFiles();
  }

  void setDocStore(int offset, String segment, boolean isCompoundFile) {        
    docStoreOffset = offset;
    docStoreSegment = segment;
    docStoreIsCompoundFile = isCompoundFile;
  }
  
  /**
   * Save this segment's info.
   */
  void write(IndexOutput output)
    throws IOException {
    output.writeString(name);
    output.writeInt(docCount);
    output.writeLong(delGen);
    output.writeInt(docStoreOffset);
    if (docStoreOffset != -1) {
      output.writeString(docStoreSegment);
      output.writeByte((byte) (docStoreIsCompoundFile ? 1:0));
    }

    output.writeByte((byte) (hasSingleNormFile ? 1:0));
    if (normGen == null) {
      output.writeInt(NO);
    } else {
      output.writeInt(normGen.length);
      for(int j = 0; j < normGen.length; j++) {
        output.writeLong(normGen[j]);
      }
    }
    output.writeByte(isCompoundFile);
    output.writeInt(delCount);
    output.writeByte((byte) (hasProx ? 1:0));
    output.writeStringStringMap(diagnostics);
  }

  void setHasProx(boolean hasProx) {
    this.hasProx = hasProx;
    clearFiles();
  }

  public boolean getHasProx() {
    return hasProx;
  }

  private void addIfExists(List files, String fileName) throws IOException {
    if (dir.fileExists(fileName))
      files.add(fileName);
  }

  /*
   * Return all files referenced by this SegmentInfo.  The
   * returns List is a locally cached List so you should not
   * modify it.
   */

  public List files() throws IOException {

    if (files != null) {
      // Already cached:
      return files;
    }
    
    files = new ArrayList();
    
    boolean useCompoundFile = getUseCompoundFile();

    if (useCompoundFile) {
      files.add(name + "." + IndexFileNames.COMPOUND_FILE_EXTENSION);
    } else {
      final String[] exts = IndexFileNames.NON_STORE_INDEX_EXTENSIONS;
      for(int i=0;i<exts.length;i++)
        addIfExists(files, name + "." + exts[i]);
    }

    if (docStoreOffset != -1) {
      // We are sharing doc stores (stored fields, term
      // vectors) with other segments
      assert docStoreSegment != null;
      if (docStoreIsCompoundFile) {
        files.add(docStoreSegment + "." + IndexFileNames.COMPOUND_FILE_STORE_EXTENSION);
      } else {
        final String[] exts = IndexFileNames.STORE_INDEX_EXTENSIONS;
        for(int i=0;i<exts.length;i++)
          addIfExists(files, docStoreSegment + "." + exts[i]);
      }
    } else if (!useCompoundFile) {
      // We are not sharing, and, these files were not
      // included in the compound file
      final String[] exts = IndexFileNames.STORE_INDEX_EXTENSIONS;
      for(int i=0;i<exts.length;i++)
        addIfExists(files, name + "." + exts[i]);
    }

    String delFileName = IndexFileNames.fileNameFromGeneration(name, "." + IndexFileNames.DELETES_EXTENSION, delGen);
    if (delFileName != null && (delGen >= YES || dir.fileExists(delFileName))) {
      files.add(delFileName);
    }

    // Careful logic for norms files    
    if (normGen != null) {
      for(int i=0;i<normGen.length;i++) {
        long gen = normGen[i];
        if (gen >= YES) {
          // Definitely a separate norm file, with generation:
          files.add(IndexFileNames.fileNameFromGeneration(name, "." + IndexFileNames.SEPARATE_NORMS_EXTENSION + i, gen));
        } else if (NO == gen) {
          // No separate norms but maybe plain norms
          // in the non compound file case:
          if (!hasSingleNormFile && !useCompoundFile) {
            String fileName = name + "." + IndexFileNames.PLAIN_NORMS_EXTENSION + i;
            if (dir.fileExists(fileName)) {
              files.add(fileName);
            }
          }
        } else if (CHECK_DIR == gen) {
          // Pre-2.1: we have to check file existence
          String fileName = null;
          if (useCompoundFile) {
            fileName = name + "." + IndexFileNames.SEPARATE_NORMS_EXTENSION + i;
          } else if (!hasSingleNormFile) {
            fileName = name + "." + IndexFileNames.PLAIN_NORMS_EXTENSION + i;
          }
          if (fileName != null && dir.fileExists(fileName)) {
            files.add(fileName);
          }
        }
      }
    } else if (preLockless || (!hasSingleNormFile && !useCompoundFile)) {
      // Pre-2.1: we have to scan the dir to find all
      // matching _X.sN/_X.fN files for our segment:
      String prefix;
      if (useCompoundFile)
        prefix = name + "." + IndexFileNames.SEPARATE_NORMS_EXTENSION;
      else
        prefix = name + "." + IndexFileNames.PLAIN_NORMS_EXTENSION;
      int prefixLength = prefix.length();
      String[] allFiles = dir.listAll();
      final IndexFileNameFilter filter = IndexFileNameFilter.getFilter();
      for(int i=0;i<allFiles.length;i++) {
        String fileName = allFiles[i];
        if (filter.accept(null, fileName) && fileName.length() > prefixLength && Character.isDigit(fileName.charAt(prefixLength)) && fileName.startsWith(prefix)) {
          files.add(fileName);
        }
      }
    }
    return files;
  }

  /* Called whenever any change is made that affects which
   * files this segment has. */
  private void clearFiles() {
    files = null;
    sizeInBytes = -1;
  }

  /** Used for debugging */
  public String segString(Directory dir) {
    String cfs;
    try {
      if (getUseCompoundFile())
        cfs = "c";
      else
        cfs = "C";
    } catch (IOException ioe) {
      cfs = "?";
    }

    String docStore;

    if (docStoreOffset != -1)
      docStore = "->" + docStoreSegment;
    else
      docStore = "";

    return name + ":" +
      cfs +
      (this.dir == dir ? "" : "x") +
      docCount + docStore;
  }

  /** We consider another SegmentInfo instance equal if it
   *  has the same dir and same name. */
  public boolean equals(Object obj) {
    SegmentInfo other;
    try {
      other = (SegmentInfo) obj;
    } catch (ClassCastException cce) {
      return false;
    }
    return other.dir == dir && other.name.equals(name);
  }

  public int hashCode() {
    return dir.hashCode() + name.hashCode();
  }
}
