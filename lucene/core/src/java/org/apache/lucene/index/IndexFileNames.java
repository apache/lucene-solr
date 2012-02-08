package org.apache.lucene.index;

import java.util.regex.Pattern;

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
 * This class contains useful constants representing filenames and extensions
 * used by lucene, as well as convenience methods for querying whether a file
 * name matches an extension ({@link #matchesExtension(String, String)
 * matchesExtension}), as well as generating file names from a segment name,
 * generation and extension (
 * {@link #fileNameFromGeneration(String, String, long) fileNameFromGeneration},
 * {@link #segmentFileName(String, String) segmentFileName}).
 * 
 * @lucene.internal
 */
public final class IndexFileNames {

  /** Name of the index segment file */
  public static final String SEGMENTS = "segments";

  /** Name of the generation reference file name */
  public static final String SEGMENTS_GEN = "segments.gen";
  
  /** Name of the index deletable file (only used in
   * pre-lockless indices) */
  public static final String DELETABLE = "deletable";
   
  /** Extension of norms file */
  public static final String NORMS_EXTENSION = "nrm";

  /** Extension of freq postings file */
  public static final String FREQ_EXTENSION = "frq";

  /** Extension of prox postings file */
  public static final String PROX_EXTENSION = "prx";

  /** Extension of terms file */
  public static final String TERMS_EXTENSION = "tis";

  /** Extension of terms index file */
  public static final String TERMS_INDEX_EXTENSION = "tii";

  /** Extension of stored fields index file */
  public static final String FIELDS_INDEX_EXTENSION = "fdx";

  /** Extension of stored fields file */
  public static final String FIELDS_EXTENSION = "fdt";

  /** Extension of vectors fields file */
  public static final String VECTORS_FIELDS_EXTENSION = "tvf";

  /** Extension of vectors documents file */
  public static final String VECTORS_DOCUMENTS_EXTENSION = "tvd";

  /** Extension of vectors index file */
  public static final String VECTORS_INDEX_EXTENSION = "tvx";

  /** Extension of compound file */
  public static final String COMPOUND_FILE_EXTENSION = "cfs";

  /** Extension of compound file for doc store files*/
  public static final String COMPOUND_FILE_STORE_EXTENSION = "cfx";

  /** Extension of deletes */
  public static final String DELETES_EXTENSION = "del";

  /** Extension of field infos */
  public static final String FIELD_INFOS_EXTENSION = "fnm";

  /** Extension of plain norms */
  public static final String PLAIN_NORMS_EXTENSION = "f";

  /** Extension of separate norms */
  public static final String SEPARATE_NORMS_EXTENSION = "s";

  /** Extension of gen file */
  public static final String GEN_EXTENSION = "gen";

  /**
   * This array contains all filename extensions used by
   * Lucene's index files, with two exceptions, namely the
   * extension made up from <code>.f</code> + a number and
   * from <code>.s</code> + a number.  Also note that
   * Lucene's <code>segments_N</code> files do not have any
   * filename extension.
   */
  public static final String INDEX_EXTENSIONS[] = new String[] {
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
  public static final String[] INDEX_EXTENSIONS_IN_COMPOUND_FILE = new String[] {
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

  public static final String[] STORE_INDEX_EXTENSIONS = new String[] {
    VECTORS_INDEX_EXTENSION,
    VECTORS_FIELDS_EXTENSION,
    VECTORS_DOCUMENTS_EXTENSION,
    FIELDS_INDEX_EXTENSION,
    FIELDS_EXTENSION
  };

  public static final String[] NON_STORE_INDEX_EXTENSIONS = new String[] {
    FIELD_INFOS_EXTENSION,
    FREQ_EXTENSION,
    PROX_EXTENSION,
    TERMS_EXTENSION,
    TERMS_INDEX_EXTENSION,
    NORMS_EXTENSION
  };
  
  /** File extensions of old-style index files */
  public static final String COMPOUND_EXTENSIONS[] = new String[] {
    FIELD_INFOS_EXTENSION,
    FREQ_EXTENSION,
    PROX_EXTENSION,
    FIELDS_INDEX_EXTENSION,
    FIELDS_EXTENSION,
    TERMS_INDEX_EXTENSION,
    TERMS_EXTENSION
  };
  
  /** File extensions for term vector support */
  public static final String VECTOR_EXTENSIONS[] = new String[] {
    VECTORS_INDEX_EXTENSION,
    VECTORS_DOCUMENTS_EXTENSION,
    VECTORS_FIELDS_EXTENSION
  };

  /**
   * Computes the full file name from base, extension and generation. If the
   * generation is -1, the file name is null. If it's 0, the file name is
   * &lt;base&gt;.&lt;ext&gt;. If it's > 0, the file name is
   * &lt;base&gt;_&lt;gen&gt;.&lt;ext&gt;.<br>
   * <b>NOTE:</b> .&lt;ext&gt; is added to the name only if <code>ext</code> is
   * not an empty string.
   * 
   * @param base main part of the file name
   * @param ext extension of the filename
   * @param gen generation
   */
  public static final String fileNameFromGeneration(String base, String ext, long gen) {
    if (gen == SegmentInfo.NO) {
      return null;
    } else if (gen == SegmentInfo.WITHOUT_GEN) {
      return segmentFileName(base, ext);
    } else {
      // The '6' part in the length is: 1 for '.', 1 for '_' and 4 as estimate
      // to the gen length as string (hopefully an upper limit so SB won't
      // expand in the middle.
      StringBuilder res = new StringBuilder(base.length() + 6 + ext.length())
          .append(base).append('_').append(Long.toString(gen, Character.MAX_RADIX));
      if (ext.length() > 0) {
        res.append('.').append(ext);
      }
      return res.toString();
    }
  }

  /**
   * Returns true if the provided filename is one of the doc store files (ends
   * with an extension in {@link #STORE_INDEX_EXTENSIONS}).
   */
  public static final boolean isDocStoreFile(String fileName) {
    if (fileName.endsWith(COMPOUND_FILE_STORE_EXTENSION))
      return true;
    for (String ext : STORE_INDEX_EXTENSIONS) {
      if (fileName.endsWith(ext))
        return true;
    }
    return false;
  }

  /**
   * Returns the file name that matches the given segment name and extension.
   * This method takes care to return the full file name in the form
   * &lt;segmentName&gt;.&lt;ext&gt;, therefore you don't need to prefix the
   * extension with a '.'.<br>
   * <b>NOTE:</b> .&lt;ext&gt; is added to the result file name only if
   * <code>ext</code> is not empty.
   */
  public static final String segmentFileName(String segmentName, String ext) {
    if (ext.length() > 0) {
      return new StringBuilder(segmentName.length() + 1 + ext.length()).append(
          segmentName).append('.').append(ext).toString();
    } else {
      return segmentName;
    }
  }
  
  /**
   * Returns true if the given filename ends with the given extension. One
   * should provide a <i>pure</i> extension, without '.'.
   */
  public static final boolean matchesExtension(String filename, String ext) {
    // It doesn't make a difference whether we allocate a StringBuilder ourself
    // or not, since there's only 1 '+' operator.
    return filename.endsWith("." + ext);
  }

  /**
   * Strips the segment file name out of the given one. If you used
   * {@link #segmentFileName} or {@link #fileNameFromGeneration} to create your
   * files, then this method simply removes whatever comes before the first '.',
   * or the second '_' (excluding both), in case of deleted docs.
   * 
   * @return the filename with the segment name removed, or the given filename
   *         if it does not contain a '.' and '_'.
   */
  public static final String stripSegmentName(String filename) {
    // If it is a .del file, there's an '_' after the first character
    int idx = filename.indexOf('_', 1);
    if (idx == -1) {
      // If it's not, strip everything that's before the '.'
      idx = filename.indexOf('.');
    }
    if (idx != -1) {
      filename = filename.substring(idx);
    }
    return filename;
  }

  /**
   * Returns true if the given filename ends with the separate norms file
   * pattern: {@code SEPARATE_NORMS_EXTENSION + "[0-9]+"}.
   */
  public static boolean isSeparateNormsFile(String filename) {
    int idx = filename.lastIndexOf('.');
    if (idx == -1) return false;
    String ext = filename.substring(idx + 1);
    return Pattern.matches(SEPARATE_NORMS_EXTENSION + "[0-9]+", ext);
  }

}
