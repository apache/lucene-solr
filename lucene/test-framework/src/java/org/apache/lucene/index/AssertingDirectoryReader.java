package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.List;

/**
 * A {@link DirectoryReader} that wraps all its subreaders with
 * {@link AssertingAtomicReader}
 */
public class AssertingDirectoryReader extends DirectoryReader {
  protected DirectoryReader in;

  public AssertingDirectoryReader(DirectoryReader in) {
    super(in.directory(), wrap(in.getSequentialSubReaders()));
    this.in = in;
  }
  
  private static AtomicReader[] wrap(List<? extends AtomicReader> readers) {
    AtomicReader[] wrapped = new AtomicReader[readers.size()];
    for (int i = 0; i < readers.size(); i++) {
      wrapped[i] = new AssertingAtomicReader(readers.get(i));
    }
    return wrapped;
  }

  @Override
  protected DirectoryReader doOpenIfChanged() throws IOException {
    DirectoryReader d = in.doOpenIfChanged();
    return d == null ? null : new AssertingDirectoryReader(d);
  }

  @Override
  protected DirectoryReader doOpenIfChanged(IndexCommit commit) throws IOException {
    DirectoryReader d = in.doOpenIfChanged(commit);
    return d == null ? null : new AssertingDirectoryReader(d);
  }

  @Override
  protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
    DirectoryReader d = in.doOpenIfChanged(writer, applyAllDeletes);
    return d == null ? null : new AssertingDirectoryReader(d);
  }

  @Override
  public long getVersion() {
    return in.getVersion();
  }

  @Override
  public boolean isCurrent() throws IOException {
    return in.isCurrent();
  }

  @Override
  public IndexCommit getIndexCommit() throws IOException {
    return in.getIndexCommit();
  }

  @Override
  protected void doClose() throws IOException {
    in.doClose();
  }
  
  @Override
  public Object getCoreCacheKey() {
    return in.getCoreCacheKey();
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return in.getCombinedCoreAndDeletesKey();
  }
}
