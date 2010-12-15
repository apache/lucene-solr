package org.apache.lucene.index.codecs.pfordelta;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.index.codecs.sep.IntStreamFactory;
import org.apache.lucene.index.codecs.sep.IntIndexInput;
import org.apache.lucene.index.codecs.sep.IntIndexOutput;

import java.io.IOException;

public class FORFactory extends IntStreamFactory {
  private final int blockSize;

  /** blockSize is only used when creating the
   *  IntIndexOutput */
  public FORFactory(int blockSize) {
    this.blockSize = blockSize;
  }

  public IntIndexInput openInput(Directory dir, String fileName, int readBufferSize) throws IOException {
    return new FORIndexInput(dir, fileName, readBufferSize);
  }

  public IntIndexOutput createOutput(Directory dir, String fileName) throws IOException {
    return new FORIndexOutput(dir, fileName, blockSize);
  }
}
