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

import java.io.IOException;

/** An IndexReader which reads multiple indexes, appending
 *  their content. */
public class MultiReader extends BaseMultiReader<IndexReader> {
  private final boolean closeSubReaders;
  
 /**
  * <p>Construct a MultiReader aggregating the named set of (sub)readers.
  * <p>Note that all subreaders are closed if this Multireader is closed.</p>
  * @param subReaders set of (sub)readers
  */
  public MultiReader(IndexReader... subReaders) throws IOException {
    this(subReaders, true);
  }

  /**
   * <p>Construct a MultiReader aggregating the named set of (sub)readers.
   * @param subReaders set of (sub)readers
   * @param closeSubReaders indicates whether the subreaders should be closed
   * when this MultiReader is closed
   */
  public MultiReader(IndexReader[] subReaders, boolean closeSubReaders) throws IOException {
    super(subReaders.clone());
    this.closeSubReaders = closeSubReaders;
    if (!closeSubReaders) {
      for (int i = 0; i < subReaders.length; i++) {
        subReaders[i].incRef();
      }
    }
  }

  @Override
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    for (int i = 0; i < subReaders.length; i++) {
      try {
        if (closeSubReaders) {
          subReaders[i].close();
        } else {
          subReaders[i].decRef();
        }
      } catch (IOException e) {
        if (ioe == null) ioe = e;
      }
    }
    // throw the first exception
    if (ioe != null) throw ioe;
  }
}
