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

import org.apache.lucene.util.BytesRef;

/**
 * Convenience class returning empty values for positions, offsets and payloads
 */
public abstract class DocsEnum extends PostingsEnum {

  /** Sole constructor. (For invocation by subclass
   *  constructors, typically implicit.) */
  protected DocsEnum() {
    super();
  }

  /**
   * @return -1, indicating no positions are available
   * @throws IOException if a low-level IO exception occurred
   */
  @Override
  public int nextPosition() throws IOException {
    return -1;
  }

  /**
   * @return -1, indicating no offsets are available
   * @throws IOException if a low-level IO exception occurred
   */
  @Override
  public int startOffset() throws IOException {
    return -1;
  }

  /**
   * @return -1, indicating no offsets are available
   * @throws IOException if a low-level IO exception occurred
   */
  @Override
  public int endOffset() throws IOException {
    return -1;
  }

  /**
   * @return null, indicating no payloads are available
   * @throws IOException if a low-level IO exception occurred
   */
  @Override
  public BytesRef getPayload() throws IOException {
    return null;
  }
}
