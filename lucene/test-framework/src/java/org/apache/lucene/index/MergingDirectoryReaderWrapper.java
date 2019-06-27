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
package org.apache.lucene.index;

import java.io.IOException;

/**
 * {@link DirectoryReader} wrapper that uses the merge instances of the wrapped
 * {@link CodecReader}s.
 * NOTE: This class will fail to work if the leaves of the wrapped directory are
 * not codec readers.
 */
public final class MergingDirectoryReaderWrapper extends FilterDirectoryReader {

  /** Wrap the given directory. */
  public MergingDirectoryReaderWrapper(DirectoryReader in) throws IOException {
    super(in, new SubReaderWrapper() {
      @Override
      public LeafReader wrap(LeafReader reader) {
        return new MergingCodecReader((CodecReader) reader);
      }
    });
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
    return new MergingDirectoryReaderWrapper(in);
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    // doesn't change the content: can delegate
    return in.getReaderCacheHelper();
  }

}
