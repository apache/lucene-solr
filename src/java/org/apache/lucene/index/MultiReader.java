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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

/** An IndexReader which reads multiple indexes, appending their content.
 *
 * @version $Id$
 */
public class MultiReader extends MultiSegmentReader {
 /**
  * <p>Construct a MultiReader aggregating the named set of (sub)readers.
  * Directory locking for delete, undeleteAll, and setNorm operations is
  * left to the subreaders. </p>
  * <p>Note that all subreaders are closed if this Multireader is closed.</p>
  * @param subReaders set of (sub)readers
  * @throws IOException
  */
  public MultiReader(IndexReader[] subReaders) throws IOException {
    super(subReaders.length == 0 ? null : subReaders[0].directory(), 
          null, false, subReaders);
  }

  /**
   * Checks recursively if all subreaders are up to date. 
   */
  public boolean isCurrent() throws CorruptIndexException, IOException {
    for (int i = 0; i < subReaders.length; i++) {
      if (!subReaders[i].isCurrent()) {
        return false;
      }
    }
    
    // all subreaders are up to date
    return true;
  }
  
  /** Not implemented.
   * @throws UnsupportedOperationException
   */
  public long getVersion() {
    throw new UnsupportedOperationException("MultiReader does not support this method.");
  }
}
