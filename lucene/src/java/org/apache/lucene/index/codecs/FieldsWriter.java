package org.apache.lucene.index.codecs;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
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

public abstract class FieldsWriter implements Closeable {

  public abstract void addDocument(Iterable<? extends IndexableField> doc, FieldInfos fieldInfos) throws IOException;
  
  /** Bulk write a contiguous series of documents.  The
   *  lengths array is the length (in bytes) of each raw
   *  document.  The stream IndexInput is the
   *  fieldsStream from which we should bulk-copy all
   *  bytes. */
  public abstract void addRawDocuments(IndexInput stream, int[] lengths, int numDocs) throws IOException;
  
  public abstract void startDocument(int numStoredFields) throws IOException;
  
  public abstract void skipDocument() throws IOException;
  
  public abstract void writeField(int fieldNumber, IndexableField field) throws IOException;

  public abstract void abort();
}
