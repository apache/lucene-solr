package org.apache.lucene.index.codecs.fixed;

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

import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexInput;
import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexOutput;
import org.apache.lucene.index.codecs.sep.IntStreamFactory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/** 
 * Specialized factory for using fixed int block codecs.
 * <p>
 * You subclass this, and can then choose to use either the Sep or Fixed
 * file formats.
 */
public abstract class FixedIntStreamFactory extends IntStreamFactory {

  // nocommit?: the below three methods are dumb: they exist so your codec can easily support sep or interleaved.
  @Override
  public final FixedIntBlockIndexInput openInput(Directory dir, String fileName) throws IOException {
    return openInput(dir, fileName, BufferedIndexInput.BUFFER_SIZE);
  }

  @Override
  public final FixedIntBlockIndexInput openInput(Directory dir, String fileName, int readBufferSize) throws IOException {
    return openInput(dir.openInput(fileName, readBufferSize), fileName, false);
  }

  @Override
  public final FixedIntBlockIndexOutput createOutput(Directory dir, String fileName) throws IOException {
    return createOutput(dir.createOutput(fileName), fileName, false);
  }
  
  // nocommit: its not good we force the codecs to wrap II/IO's, even though this is how they all work today...

  /**
   * Return a fixed block input, wrapping the underlying file.
   * If <code>isChild</code> is true, then this is the codec for child blocks
   * piggybacking upon a parent (e.g. freqs).
   */
  public abstract FixedIntBlockIndexInput openInput(IndexInput in, String fileName, boolean isChild) throws IOException;
  
  /**
   * Return a fixed block output, wrapping the underlying file.
   * If <code>isChild</code> is true, then this is the codec for child blocks
   * piggybacking upon a parent (e.g. freqs).
   */
  public abstract FixedIntBlockIndexOutput createOutput(IndexOutput out, String fileName, boolean isChild) throws IOException;
}
