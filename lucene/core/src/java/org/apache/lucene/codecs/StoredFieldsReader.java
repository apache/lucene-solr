package org.apache.lucene.codecs;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.StoredFieldVisitor;

/**
 * Codec API for reading stored fields:
 * 
 * You need to implement {@link #visitDocument(int, StoredFieldVisitor)} to
 * read the stored fields for a document, implement {@link #clone()} (creating
 * clones of any IndexInputs used, etc), and {@link #close()}
 * @lucene.experimental
 */
public abstract class StoredFieldsReader implements Cloneable, Closeable {
  
  /** Visit the stored fields for document <code>n</code> */
  public abstract void visitDocument(int n, StoredFieldVisitor visitor) throws CorruptIndexException, IOException;

  public abstract StoredFieldsReader clone();
}
