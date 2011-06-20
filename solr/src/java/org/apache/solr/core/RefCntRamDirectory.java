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

package org.apache.solr.core;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.IOContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class RefCntRamDirectory extends RAMDirectory {

  private final AtomicInteger refCount = new AtomicInteger();

  public RefCntRamDirectory() {
    super();
    refCount.set(1);
  }

  public RefCntRamDirectory(Directory dir) throws IOException {
    this();
    for (String file : dir.listAll()) {
      dir.copy(this, file, file, IOContext.DEFAULT);
    }
  }

  public void incRef() {
    ensureOpen();
    refCount.incrementAndGet();
  }

  public void decRef() {
    ensureOpen();
    if (refCount.getAndDecrement() == 1) {
      super.close();
    }
  }

  @Override
  public final synchronized void close() {
    decRef();
  }

  public boolean isOpen() {
    return isOpen;
  }

}
