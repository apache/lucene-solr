package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
  
  /** Name of the index deletable file */
  static final String DELETABLE = "deletable";
  
  /**
   * This array contains all filename extensions used by Lucene's index files, with
   * one exception, namely the extension made up from <code>.f</code> + a number.
   * Also note that two of Lucene's files (<code>deletable</code> and
   * <code>segments</code>) don't have any filename extension.
   */
  static final String INDEX_EXTENSIONS[] = new String[] {
      "cfs", "fnm", "fdx", "fdt", "tii", "tis", "frq", "prx", "del",
      "tvx", "tvd", "tvf", "tvp" };
  
  /** File extensions of old-style index files */
  static final String COMPOUND_EXTENSIONS[] = new String[] {
    "fnm", "frq", "prx", "fdx", "fdt", "tii", "tis"
  };
  
  /** File extensions for term vector support */
  static final String VECTOR_EXTENSIONS[] = new String[] {
    "tvx", "tvd", "tvf"
  };
  
}
