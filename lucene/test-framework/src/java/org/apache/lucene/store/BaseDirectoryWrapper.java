/*
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
package org.apache.lucene.store;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util.TestUtil;

/**
 * Calls check index on close.
 */
// do NOT make any methods in this class synchronized, volatile
// do NOT import anything from the concurrency package.
// no randoms, no nothing.
public abstract class BaseDirectoryWrapper extends FilterDirectory {
  
  private boolean checkIndexOnClose = true;
  private boolean crossCheckTermVectorsOnClose = true;
  protected volatile boolean isOpen = true;

  protected BaseDirectoryWrapper(Directory delegate) {
    super(delegate);
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      isOpen = false;
      if (checkIndexOnClose && DirectoryReader.indexExists(this)) {
        TestUtil.checkIndex(this, crossCheckTermVectorsOnClose);
      }
    }
    super.close();
  }
  
  public boolean isOpen() {
    return isOpen;
  }
  
  /**
   * Set whether or not checkindex should be run
   * on close
   */
  public void setCheckIndexOnClose(boolean value) {
    this.checkIndexOnClose = value;
  }
  
  public boolean getCheckIndexOnClose() {
    return checkIndexOnClose;
  }

  public void setCrossCheckTermVectorsOnClose(boolean value) {
    this.crossCheckTermVectorsOnClose = value;
  }

  public boolean getCrossCheckTermVectorsOnClose() {
    return crossCheckTermVectorsOnClose;
  }
}
