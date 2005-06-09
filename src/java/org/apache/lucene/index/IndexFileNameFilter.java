package org.apache.lucene.index;

/**
 * Copyright 2005 The Apache Software Foundation
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

import java.io.File;
import java.io.FilenameFilter;

/**
 * Filename filter that accept filenames and extensions only created by Lucene.
 * 
 * @author Daniel Naber / Bernhard Messer
 * @version $rcs = ' $Id: Exp $ ' ;
 */
public class IndexFileNameFilter implements FilenameFilter {

  /* (non-Javadoc)
   * @see java.io.FilenameFilter#accept(java.io.File, java.lang.String)
   */
  public boolean accept(File dir, String name) {
    for (int i = 0; i < IndexFileNames.INDEX_EXTENSIONS.length; i++) {
      if (name.endsWith("."+IndexFileNames.INDEX_EXTENSIONS[i]))
        return true;
    }
    if (name.equals(IndexFileNames.DELETABLE)) return true;
    else if (name.equals(IndexFileNames.SEGMENTS)) return true;
    else if (name.matches(".+\\.f\\d+")) return true;
    return false;
  }

}
