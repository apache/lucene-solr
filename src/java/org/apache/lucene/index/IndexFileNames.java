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
 * @author Bernhard Messer
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
  
  /**
   * This array contains all filename extensions used by
   * Lucene's index files, with two exceptions, namely the
   * extension made up from <code>.f</code> + a number and
   * from <code>.s</code> + a number.  Also note that
   * Lucene's <code>segments_N</code> files do not have any
   * filename extension.
   */
  static final String INDEX_EXTENSIONS[] = new String[] {
      "cfs", "fnm", "fdx", "fdt", "tii", "tis", "frq", "prx", "del",
      "tvx", "tvd", "tvf", "gen", "nrm" 
  };
  
  /** File extensions of old-style index files */
  static final String COMPOUND_EXTENSIONS[] = new String[] {
    "fnm", "frq", "prx", "fdx", "fdt", "tii", "tis"
  };
  
  /** File extensions for term vector support */
  static final String VECTOR_EXTENSIONS[] = new String[] {
    "tvx", "tvd", "tvf"
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
  public static final String fileNameFromGeneration(String base, String extension, long gen) {
    if (gen == -1) {
      return null;
    } else if (gen == 0) {
      return base + extension;
    } else {
      return base + "_" + Long.toString(gen, Character.MAX_RADIX) + extension;
    }
  }
}
