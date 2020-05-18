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
package org.apache.lucene.codecs;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

/**
 * A read-only {@link Directory} that consists of a view over a compound file.
 * @see CompoundFormat
 * @lucene.experimental
 */
public abstract class CompoundDirectory extends Directory {

  /** Sole constructor. */
  protected CompoundDirectory() {}

  /**
   * Checks consistency of this directory.
   * <p>
   * Note that this may be costly in terms of I/O, e.g.
   * may involve computing a checksum value against large data files.
   */
  public abstract void checkIntegrity() throws IOException;

  /** Not implemented
   * @throws UnsupportedOperationException always: not supported by CFS */
  @Override
  public final void deleteFile(String name) {
    throw new UnsupportedOperationException();
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException always: not supported by CFS */
  @Override
  public final void rename(String from, String to) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void syncMetaData() {
  }

  @Override
  public final IndexOutput createOutput(String name, IOContext context) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public final void sync(Collection<String> names) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public final Lock obtainLock(String name) {
    throw new UnsupportedOperationException();
  }

}
