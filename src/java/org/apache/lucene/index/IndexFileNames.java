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

/**
 * Useful constants representing filenames and extensions used by lucene
 *
 * @version $rcs = ' $Id: Exp $ ' ;
 */
final class IndexFileNames {

  /** Name of the index segment file */
  static final String SEGMENTS = "segments";

  /** Name of the generation reference file name */
  static final String SEGMENTS_GEN = "segments.gen";
  
  /** Name of the index deletable file (only used in
   * pre-lockless indices) */
  static final String DELETABLE = "deletable";
   
  /** Extension of norms file */
  static final String NORMS_EXTENSION = "nrm";

  /** Extension of freq postings file */
  static final String FREQ_EXTENSION = "frq";

  /** Extension of prox postings file */
  static final String PROX_EXTENSION = "prx";

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tis";

  /** Extension of terms index file */
  static final String TERMS_INDEX_EXTENSION = "tii";

  /** Extension of stored fields index file */
  static final String FIELDS_INDEX_EXTENSION = "fdx";

  /** Extension of stored fields file */
  static final String FIELDS_EXTENSION = "fdt";

  /** Extension of vectors fields file */
  static final String VECTORS_FIELDS_EXTENSION = "tvf";

  /** Extension of vectors documents file */
  static final String VECTORS_DOCUMENTS_EXTENSION = "tvd";

  /** Extension of vectors index file */
  static final String VECTORS_INDEX_EXTENSION = "tvx";

  /** Extension of compound file */
  static final String COMPOUND_FILE_EXTENSION = "cfs";

  /** Extension of compound file for doc store files*/
  static final String COMPOUND_FILE_STORE_EXTENSION = "cfx";

  /** Extension of deletes */
  static final String DELETES_EXTENSION = "del";

  /** Extension of field infos */
  static final String FIELD_INFOS_EXTENSION = "fnm";

  /** Extension of plain norms */
  static final String PLAIN_NORMS_EXTENSION = "f";

  /** Extension of separate norms */
  static final String SEPARATE_NORMS_EXTENSION = "s";

  /** Extension of gen file */
  static final String GEN_EXTENSION = "gen";

  /**
   * This array contains all filename extensions used by
   * Lucene's index files, with two exceptions, namely the
   * extension made up from <code>.f</code> + a number and
   * from <code>.s</code> + a number.  Also note that
   * Lucene's <code>segments_N</code> files do not have any
   * filename extension.
   */
  static final String INDEX_EXTENSIONS[] = new String[] {
    COMPOUND_FILE_EXTENSION,
    FIELD_INFOS_EXTENSION,
    FIELDS_INDEX_EXTENSION,
    FIELDS_EXTENSION,
    TERMS_INDEX_EXTENSION,
    TERMS_EXTENSION,
    FREQ_EXTENSION,
    PROX_EXTENSION,
    DELETES_EXTENSION,
    VECTORS_INDEX_EXTENSION,
    VECTORS_DOCUMENTS_EXTENSION,
    VECTORS_FIELDS_EXTENSION,
    GEN_EXTENSION,
    NORMS_EXTENSION,
    COMPOUND_FILE_STORE_EXTENSION,
  };

  /** File extensions that are added to a compound file
   * (same as above, minus "del", "gen", "cfs"). */
  static final String[] INDEX_EXTENSIONS_IN_COMPOUND_FILE = new String[] {
    FIELD_INFOS_EXTENSION,
    FIELDS_INDEX_EXTENSION,
    FIELDS_EXTENSION,
    TERMS_INDEX_EXTENSION,
    TERMS_EXTENSION,
    FREQ_EXTENSION,
    PROX_EXTENSION,
    VECTORS_INDEX_EXTENSION,
    VECTORS_DOCUMENTS_EXTENSION,
    VECTORS_FIELDS_EXTENSION,
    NORMS_EXTENSION
  };

  static final String[] STORE_INDEX_EXTENSIONS = new String[] {
    VECTORS_INDEX_EXTENSION,
    VECTORS_FIELDS_EXTENSION,
    VECTORS_DOCUMENTS_EXTENSION,
    FIELDS_INDEX_EXTENSION,
    FIELDS_EXTENSION
  };

  static final String[] NON_STORE_INDEX_EXTENSIONS = new String[] {
    FIELD_INFOS_EXTENSION,
    FREQ_EXTENSION,
    PROX_EXTENSION,
    TERMS_EXTENSION,
    TERMS_INDEX_EXTENSION,
    NORMS_EXTENSION
  };
  
  /** File extensions of old-style index files */
  static final String COMPOUND_EXTENSIONS[] = new String[] {
    FIELD_INFOS_EXTENSION,
    FREQ_EXTENSION,
    PROX_EXTENSION,
    FIELDS_INDEX_EXTENSION,
    FIELDS_EXTENSION,
    TERMS_INDEX_EXTENSION,
    TERMS_EXTENSION
  };
  
  /** File extensions for term vector support */
  static final String VECTOR_EXTENSIONS[] = new String[] {
    VECTORS_INDEX_EXTENSION,
    VECTORS_DOCUMENTS_EXTENSION,
    VECTORS_FIELDS_EXTENSION
  };

  /**
   * Computes the full file name from base, extension and
   * generation.  If the generation is -1, the file name is
   * null.  If it's 0, the file name is <base><extension>.
   * If it's > 0, the file name is <base>_<generation><extension>.
   *
   * @param base -- main part of the file name
   * @param extension -- extension of the filename (including .)
   * @param gen -- generation
   */
  static final String fileNameFromGeneration(String base, String extension, long gen) {
    if (gen == SegmentInfo.NO) {
      return null;
    } else if (gen == SegmentInfo.WITHOUT_GEN) {
      return base + extension;
    } else {
      return base + "_" + Long.toString(gen, Character.MAX_RADIX) + extension;
    }
  }

  /**
   * Returns true if the provided filename is one of the doc
   * store files (ends with an extension in
   * STORE_INDEX_EXTENSIONS).
   */
  static final boolean isDocStoreFile(String fileName) {
    if (fileName.endsWith(COMPOUND_FILE_STORE_EXTENSION))
      return true;
    for(int i=0;i<STORE_INDEX_EXTENSIONS.length;i++)
      if (fileName.endsWith(STORE_INDEX_EXTENSIONS[i]))
        return true;
    return false;
  }
}
