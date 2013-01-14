package org.apache.lucene.facet.associations;

import java.io.IOException;

import org.apache.lucene.facet.search.PayloadIterator;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
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
public abstract class AssociationsPayloadIterator<T extends CategoryAssociation> {

  private final PayloadIterator pi;
  private final T association;
  
  /**
   * Marking whether there are associations (at all) in the given index
   */
  private boolean hasAssociations = false;

  /**
   * Construct a new associations iterator. The given
   * {@link CategoryAssociation} is used to deserialize the association values.
   * It is assumed that all association values can be deserialized with the
   * given {@link CategoryAssociation}.
   */
  public AssociationsPayloadIterator(String field, T association) throws IOException {
    pi = new PayloadIterator(new Term(field, association.getCategoryListID()));
    this.association = association;
  }

  /**
   * Sets the {@link AtomicReaderContext} for which {@link #setNextDoc(int)}
   * calls will be made. Returns true iff this reader has associations for any
   * of the documents belonging to the association given to the constructor.
   */
  public final boolean setNextReader(AtomicReaderContext context) throws IOException {
    hasAssociations = pi.setNextReader(context);
    return hasAssociations;
  }
  
  /**
   * Skip to the requested document. Returns true iff the document has category
   * association values and they were read successfully. Associations are
   * handled through {@link #handleAssociation(int, CategoryAssociation)} by
   * extending classes.
   */
  protected final boolean setNextDoc(int docID) throws IOException {
    if (!hasAssociations) { // there are no associations at all
      return false;
    }

    BytesRef bytes = pi.getPayload(docID);
    if (bytes == null) { // no associations for the requested document
      return false;
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
