package org.apache.lucene.index;

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

/**
 * {@link IndexReaderContext} for {@link CompositeReader} instance.
 * @lucene.experimental
 */
public final class CompositeReaderContext extends IndexReaderContext {
  private final IndexReaderContext[] children;
  private final AtomicReaderContext[] leaves;
  private final CompositeReader reader;

  /**
   * Creates a {@link CompositeReaderContext} for intermediate readers that aren't
   * not top-level readers in the current context
   */
  CompositeReaderContext(CompositeReaderContext parent, CompositeReader reader,
      int ordInParent, int docbaseInParent, IndexReaderContext[] children) {
    this(parent, reader, ordInParent, docbaseInParent, children, null);
  }
  
  /**
   * Creates a {@link CompositeReaderContext} for top-level readers with parent set to <code>null</code>
   */
  CompositeReaderContext(CompositeReader reader, IndexReaderContext[] children, AtomicReaderContext[] leaves) {
    this(null, reader, 0, 0, children, leaves);
  }
  
  private CompositeReaderContext(CompositeReaderContext parent, CompositeReader reader,
      int ordInParent, int docbaseInParent, IndexReaderContext[] children,
      AtomicReaderContext[] leaves) {
    super(parent, ordInParent, docbaseInParent);
    this.children = children;
    this.leaves = leaves;
    this.reader = reader;
  }

  @Override
  public AtomicReaderContext[] leaves() {
    return leaves;
  }
  
  
  @Override
  public IndexReaderContext[] children() {
    return children;
  }
  
  @Override
  public CompositeReader reader() {
    return reader;
  }
}