package org.apache.lucene.facet.associations;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;

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

/**
 * An iterator over a document's category associations.
 * 
 * @lucene.experimental
 */
public abstract class AssociationsIterator<T extends CategoryAssociation> {

  private final T association;
  private final String dvField;
  private final BytesRef bytes = new BytesRef(32);
  
  private BinaryDocValues current;
  
  /**
   * Construct a new associations iterator. The given
   * {@link CategoryAssociation} is used to deserialize the association values.
   * It is assumed that all association values can be deserialized with the
   * given {@link CategoryAssociation}.
   */
  public AssociationsIterator(String field, T association) throws IOException {
    this.association = association;
    this.dvField = field + association.getCategoryListID();
  }

  /**
   * Sets the {@link AtomicReaderContext} for which {@link #setNextDoc(int)}
   * calls will be made. Returns true iff this reader has associations for any
   * of the documents belonging to the association given to the constructor.
   */
  public final boolean setNextReader(AtomicReaderContext context) throws IOException {
    current = context.reader().getBinaryDocValues(dvField);
    return current != null;
  }
  
  /**
   * Skip to the requested document. Returns true iff the document has category
   * association values and they were read successfully. Associations are
   * handled through {@link #handleAssociation(int, CategoryAssociation)} by
   * extending classes.
   */
  protected final boolean setNextDoc(int docID) throws IOException {
    current.get(docID, bytes);
    if (bytes.length == 0) {
      return false; // no associations for the requested document
    }

    ByteArrayDataInput in = new ByteArrayDataInput(bytes.bytes, bytes.offset, bytes.length);
    while (!in.eof()) {
      int ordinal = in.readInt();
      association.deserialize(in);
      handleAssociation(ordinal, association);
    }
    return true;
  }

  /** A hook for extending classes to handle the given association value for the ordinal. */
  protected abstract void handleAssociation(int ordinal, T association);

}
