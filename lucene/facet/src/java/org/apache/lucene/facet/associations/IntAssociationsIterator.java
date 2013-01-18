package org.apache.lucene.facet.associations;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.util.collections.IntToIntMap;

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
 * An {@link AssociationsIterator} over integer association values.
 * 
 * @lucene.experimental
 */
public class IntAssociationsIterator extends AssociationsIterator<CategoryIntAssociation> {

  private final IntToIntMap ordinalAssociations = new IntToIntMap();

  /**
   * Constructs a new {@link IntAssociationsIterator} which uses an
   * in-memory {@link DocValues#getSource() DocValues source}.
   */
  public IntAssociationsIterator(String field, CategoryIntAssociation association) throws IOException {
    this(field, association, false);
  }

  /**
   * Constructs a new {@link IntAssociationsIterator} which uses a
   * {@link DocValues} {@link Source} per {@code useDirectSource}.
   */
  public IntAssociationsIterator(String field, CategoryIntAssociation association, boolean useDirectSource)
      throws IOException {
    super(field, association, useDirectSource);
  }

  @Override
  protected void handleAssociation(int ordinal, CategoryIntAssociation association) {
    ordinalAssociations.put(ordinal, association.getValue());
  }
  
  /**
   * Returns the integer association values of the categories that are
   * associated with the given document, or {@code null} if the document has no
   * associations.
   * <p>
   * <b>NOTE:</b> you are not expected to modify the returned map.
   */
  public IntToIntMap getAssociations(int docID) throws IOException {
    ordinalAssociations.clear();
    return setNextDoc(docID) ? ordinalAssociations : null;
  }

}
