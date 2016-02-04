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
import java.util.Random;

/**
 * A {@link DirectoryReader} that wraps all its subreaders with
 * {@link MismatchedLeafReader}
 */
public class MismatchedDirectoryReader extends FilterDirectoryReader {

  static class MismatchedSubReaderWrapper extends SubReaderWrapper {
    final Random random;
    
    MismatchedSubReaderWrapper(Random random) {
      this.random = random;
    }
    
    @Override
    public LeafReader wrap(LeafReader reader) {
      return new MismatchedLeafReader(reader, random);
    }
  }

  public MismatchedDirectoryReader(DirectoryReader in, Random random) throws IOException {
    super(in, new MismatchedSubReaderWrapper(random));
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
    return new AssertingDirectoryReader(in);
  }
}
