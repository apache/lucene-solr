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
 * A struct like class that represents a hierarchical relationship between
 * {@link IndexReader} instances. 
 * @lucene.experimental
 */
public abstract class IndexReaderContext {
  /** The reader context for this reader's immediate parent, or null if none */
  public final CompositeReaderContext parent;
  /** <code>true</code> if this context struct represents the top level reader within the hierarchical context */
  public final boolean isTopLevel;
  /** the doc base for this reader in the parent, <tt>0</tt> if parent is null */
  public final int docBaseInParent;
  /** the ord for this reader in the parent, <tt>0</tt> if parent is null */
  public final int ordInParent;
  
  IndexReaderContext(CompositeReaderContext parent, int ordInParent, int docBaseInParent) {
    if (!(this instanceof CompositeReaderContext || this instanceof AtomicReaderContext))
      throw new Error("This class should never be extended by custom code!");
    this.parent = parent;
    this.docBaseInParent = docBaseInParent;
    this.ordInParent = ordInParent;
    this.isTopLevel = parent==null;
  }
  
  /** Returns the {@link IndexReader}, this context represents. */
  public abstract IndexReader reader();
  
  /**
   * Returns the context's leaves if this context is a top-level context
   * otherwise <code>null</code>. For convenience, if this is an
   * {@link AtomicReaderContext} this returns itsself as the only leaf.
   * <p>Note: this is convenience method since leaves can always be obtained by
   * walking the context tree.
   * <p><b>Warning:</b> Don't modify the returned array!
   * Doing so will corrupt the internal structure of this
   * {@code IndexReaderContext}.
   */
  public abstract AtomicReaderContext[] leaves();
  
  /**
   * Returns the context's children iff this context is a composite context
   * otherwise <code>null</code>.
   * <p><b>Warning:</b> Don't modify the returned array!
   * Doing so will corrupt the internal structure of this
   * {@code IndexReaderContext}.
   */
  public abstract IndexReaderContext[] children();
}