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

/**
 * A {@link DirectoryReader} that wraps all its subreaders with
 * {@link AssertingAtomicReader}
 */
public class AssertingDirectoryReader extends FilterDirectoryReader {

  static class AssertingSubReaderWrapper extends SubReaderWrapper {
    @Override
    public AtomicReader wrap(AtomicReader reader) {
      return new AssertingAtomicReader(reader);
    }
  }

  public AssertingDirectoryReader(DirectoryReader in) {
    super(in, new AssertingSubReaderWrapper());
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
    return new AssertingDirectoryReader(in);
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
