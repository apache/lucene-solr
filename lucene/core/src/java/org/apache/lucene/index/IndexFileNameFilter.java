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
import java.util.regex.Pattern;

/**
 * Filename filter that attempts to accept only filenames
 * created by Lucene.  Note that this is a "best effort"
 * process.  If a file is used in a Lucene index, it will
 * always match the file; but if a file is not used in a
 * Lucene index but is named in a similar way to Lucene's
 * files then this filter may accept the file.
 *
 * <p>This does not accept <code>*-write.lock</code> files.
 *
 * @lucene.internal
 */

public class IndexFileNameFilter implements FilenameFilter {

  public static final FilenameFilter INSTANCE = new IndexFileNameFilter();
  
  private IndexFileNameFilter() {
  }

  // Approximate match for files that seem to be Lucene
  // index files.  This can easily over-match, ie if some
  // app names a file _foo_bar.go:
  private final Pattern luceneFilePattern = Pattern.compile("^_[a-z0-9]+(_[a-z0-9]+)?\\.[a-z0-9]+$");

  /* (non-Javadoc)
   * @see java.io.FilenameFilter#accept(java.io.File, java.lang.String)
   */
  public boolean accept(File dir, String name) {
    if (name.lastIndexOf('.') != -1) {
      // Has an extension
      return luceneFilePattern.matcher(name).matches();
    } else {
      // No extension -- only segments_N file;
      return name.startsWith(IndexFileNames.SEGMENTS);
    }
  }
}
