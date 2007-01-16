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
import java.io.IOException;

final class SegmentInfo {
  public String name;				  // unique name in dir
  public int docCount;				  // number of docs in seg
  public Directory dir;				  // where segment resides

  private boolean preLockless;                    // true if this is a segments file written before
                                                  // lock-less commits (2.1)

  private long delGen;                            // current generation of del file; -1 if there
                                                  // are no deletes; 0 if it's a pre-2.1 segment
                                                  // (and we must check filesystem); 1 or higher if
                                                  // there are deletes at generation N
   
  private long[] normGen;                         // current generations of each field's norm file.
                                                  // If this array is null, we must check filesystem
                                                  // when preLockLess is true.  Else,
                                                  // there are no separate norms

  private byte isCompoundFile;                    // -1 if it is not; 1 if it is; 0 if it's
                                                  // pre-2.1 (ie, must check file system to see
                                                  // if <name>.cfs and <name>.nrm exist)         

  private boolean hasSingleNormFile;              // true if this segment maintains norms in a single file; 
                                                  // false otherwise
                                                  // this is currently false for segments populated by DocumentWriter
                                                  // and true for newly created merged segments (both
                                                  // compound and non compound).
  
  public SegmentInfo(String name, int docCount, Directory dir) {
    this.name = name;
    this.docCount = docCount;
    this.dir = dir;
    delGen = -1;
    isCompoundFile = 0;
    preLockless = true;
    hasSingleNormFile = false;
  }

  public SegmentInfo(String name, int docCount, Directory dir, boolean isCompoundFile, boolean hasSingleNormFile) { 
    this(name, docCount, dir);
    this.isCompoundFile = (byte) (isCompoundFile ? 1 : -1);
    this.hasSingleNormFile = hasSingleNormFile;
    preLockless = false;
  }

  /**
   * Copy everything from src SegmentInfo into our instance.
   */
  void reset(SegmentInfo src) {
    name = src.name;
    docCount = src.docCount;
    dir = src.dir;
    preLockless = src.preLockless;
    delGen = src.delGen;
    if (src.normGen == null) {
      normGen = null;
    } else {
      normGen = new long[src.normGen.length];
      System.arraycopy(src.normGen, 0, normGen, 0, src.normGen.length);
    }
    isCompoundFile = src.isCompoundFile;
    hasSingleNormFile = src.hasSingleNormFile;
  }

  /**
   * Construct a new SegmentInfo instance by reading a
   * previously saved SegmentInfo from input.
   *
   * @param dir directory to load from
   * @param format format of the segments info file
   * @param input input handle to read segment info from
   */
  public SegmentInfo(Directory dir, int format, IndexInput input) throws IOException {
    this.dir = dir;
    name = input.readString();
    docCount = input.readInt();
    if (format <= SegmentInfos.FORMAT_LOCKLESS) {
      delGen = input.readLong();
      if (format <= SegmentInfos.FORMAT_SINGLE_NORM_FILE) {
        hasSingleNormFile = (1 == input.readByte());
      } else {
        hasSingleNormFile = false;
      }
      int numNormGen = input.readInt();
      if (numNormGen == -1) {
        normGen = null;
      } else {
        normGen = new long[numNormGen];
        for(int j=0;j<numNormGen;j++) {
          normGen[j] = input.readLong();
        }
      }
      isCompoundFile = input.readByte();
      preLockless = isCompoundFile == 0;
    } else {
      delGen = 0;
      normGen = null;
      isCompoundFile = 0;
      preLockless = true;
      hasSingleNormFile = false;
    }
  }
  
  void setNumFields(int numFields) {
    if (normGen == null) {
      // normGen is null if we loaded a pre-2.1 segment
      // file, or, if this segments file hasn't had any
      // norms set against it yet:
      normGen = new long[numFields];

      if (!preLockless) {
        // This is a FORMAT_LOCKLESS segment, which means
        // there are no norms:
        for(int i=0;i<numFields;i++) {
          normGen[i] = -1;
        }
      }
    }
  }

  boolean hasDeletions()
    throws IOException {
    // Cases:
    //
    //   delGen == -1: this means this segment was written
    //     by the LOCKLESS code and for certain does not have
    //     deletions yet
    //
    //   delGen == 0: this means this segment was written by
    //     pre-LOCKLESS code which means we must check
    //     directory to see if .del file exists
    //
    //   delGen > 0: this means this segment was written by
    //     the LOCKLESS code and for certain has
    //     deletions
    //
    if (delGen == -1) {
      return false;
    } else if (delGen > 0) {
      return true;
    } else {
      return dir.fileExists(getDelFileName());
    }
  }

  void advanceDelGen() {
    // delGen 0 is reserved for pre-LOCKLESS format
    if (delGen == -1) {
      delGen = 1;
    } else {
      delGen++;
    }
  }

  void clearDelGen() {
    delGen = -1;
  }

  public Object clone () {
    SegmentInfo si = new SegmentInfo(name, docCount, dir);
    si.isCompoundFile = isCompoundFile;
    si.delGen = delGen;
    si.preLockless = preLockless;
    si.hasSingleNormFile = hasSingleNormFile;
    if (normGen != null) {
      si.normGen = (long[]) normGen.clone();
    }
    return si;
  }

  String getDelFileName() {
    if (delGen == -1) {
      // In this case we know there is no deletion filename
      // against this segment
      return null;
    } else {
      // If delGen is 0, it's the pre-lockless-commit file format
      return IndexFileNames.fileNameFromGeneration(name, ".del", delGen);
    }
  }

  /**
   * Returns true if this field for this segment has saved a separate norms file (_<segment>_N.sX).
   *
   * @param fieldNumber the field index to check
   */
  boolean hasSeparateNorms(int fieldNumber)
    throws IOException {
    if ((normGen == null && preLockless) || (normGen != null && normGen[fieldNumber] == 0)) {
      // Must fallback to directory file exists check:
      String fileName = name + ".s" + fieldNumber;
      return dir.fileExists(fileName);
    } else if (normGen == null || normGen[fieldNumber] == -1) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Returns true if any fields in this segment have separate norms.
   */
  boolean hasSeparateNorms()
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
      // code so we first check whether any normGen's are >
      // 0 (meaning they definitely have separate norms):
      for(int i=0;i<normGen.length;i++) {
        if (normGen[i] > 0) {
          return true;
        }
      }
      // Next we look for any == 0.  These cases were
      // pre-LOCKLESS and must be checked in directory:
      for(int i=0;i<normGen.length;i++) {
        if (normGen[i] == 0) {
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
    if (normGen[fieldIndex] == -1) {
      normGen[fieldIndex] = 1;
    } else {
      normGen[fieldIndex]++;
    }
  }

  /**
   * Get the file name for the norms file for this field.
   *
   * @param number field index
   */
  String getNormFileName(int number) throws IOException {
    String prefix;

    long gen;
    if (normGen == null) {
      gen = 0;
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
      return IndexFileNames.fileNameFromGeneration(name, prefix, 0);
    }
      
    // case 3: norm file for each field
    prefix = ".f";
    return IndexFileNames.fileNameFromGeneration(name, prefix + number, 0);
  }

  /**
   * Mark whether this segment is stored as a compound file.
   *
   * @param isCompoundFile true if this is a compound file;
   * else, false
   */
  void setUseCompoundFile(boolean isCompoundFile) {
    if (isCompoundFile) {
      this.isCompoundFile = 1;
    } else {
      this.isCompoundFile = -1;
    }
  }

  /**
   * Returns true if this segment is stored as a compound
   * file; else, false.
   */
  boolean getUseCompoundFile() throws IOException {
    if (isCompoundFile == -1) {
      return false;
    } else if (isCompoundFile == 1) {
      return true;
    } else {
      return dir.fileExists(name + ".cfs");
    }
  }
  
  /**
   * Save this segment's info.
   */
  void write(IndexOutput output)
    throws IOException {
    output.writeString(name);
    output.writeInt(docCount);
    output.writeLong(delGen);
    output.writeByte((byte) (hasSingleNormFile ? 1:0));
    if (normGen == null) {
      output.writeInt(-1);
    } else {
      output.writeInt(normGen.length);
      for(int j = 0; j < normGen.length; j++) {
        output.writeLong(normGen[j]);
      }
    }
    output.writeByte(isCompoundFile);
  }
}
