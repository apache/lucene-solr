package org.apache.lucene.index.codecs;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.IndexInput;

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

public abstract class FieldsReader implements Cloneable, Closeable {
  
  public abstract void visitDocument(int n, StoredFieldVisitor visitor) throws CorruptIndexException, IOException;
  
  /** Returns the length in bytes of each raw document in a
   *  contiguous range of length numDocs starting with
   *  startDocID.  Returns the IndexInput (the fieldStream),
   *  already seeked to the starting point for startDocID.*/
  public abstract IndexInput rawDocs(int[] lengths, int startDocID, int numDocs) throws IOException;

  public abstract int size();

  public abstract FieldsReader clone();
}
