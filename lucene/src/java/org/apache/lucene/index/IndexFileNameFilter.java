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

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashSet;
import org.apache.lucene.index.codecs.CodecProvider;

/**
 * Filename filter that accept filenames and extensions only
 * created by Lucene.
 *
 * @lucene.internal
 */

public class IndexFileNameFilter implements FilenameFilter {

  private final HashSet<String> extensions;

  public IndexFileNameFilter(CodecProvider codecs) {
    extensions = new HashSet<String>();
    for (String ext : IndexFileNames.INDEX_EXTENSIONS) {
      extensions.add(ext);
    }
    if (codecs != null) {
      for(String ext : codecs.getAllExtensions()) {
        extensions.add(ext);
      }
    }
  }

  /* (non-Javadoc)
   * @see java.io.FilenameFilter#accept(java.io.File, java.lang.String)
   */
  public boolean accept(File dir, String name) {
    int i = name.lastIndexOf('.');
    if (i != -1) {
      String extension = name.substring(1+i);
      if (extensions.contains(extension)) {
        return true;
      } else if (extension.startsWith("f") &&
                 extension.matches("f\\d+")) {
        return true;
      } else if (extension.startsWith("s") &&
                 extension.matches("s\\d+")) {
        return true;
      }
    } else {
      if (name.startsWith(IndexFileNames.SEGMENTS)) return true;
    }
    return false;
  }
}
