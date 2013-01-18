package org.apache.lucene.facet.associations;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.util.collections.IntToFloatMap;

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
public class FloatAssociationsIterator extends AssociationsIterator<CategoryFloatAssociation> {

  private final IntToFloatMap ordinalAssociations = new IntToFloatMap();

  /**
   * Constructs a new {@link FloatAssociationsIterator} which uses an
   * in-memory {@link DocValues#getSource() DocValues source}.
   */
  public FloatAssociationsIterator(String field, CategoryFloatAssociation association) throws IOException {
    this(field, association, false);
  }
  
  /**
   * Constructs a new {@link FloatAssociationsIterator} which uses a
   * {@link DocValues} {@link Source} per {@code useDirectSource}.
   */
  public FloatAssociationsIterator(String field, CategoryFloatAssociation association, boolean useDirectSource) 
      throws IOException {
    super(field, association, useDirectSource);
  }

  @Override
  protected void handleAssociation(int ordinal, CategoryFloatAssociation association) {
    ordinalAssociations.put(ordinal, association.getValue());
  }

  /**
   * Returns the float association values of the categories that are associated
   * with the given document, or {@code null} if the document has no
   * associations.
   * <p>
   * <b>NOTE:</b> you are not expected to modify the returned map.
   */
  public IntToFloatMap getAssociations(int docID) throws IOException {
    ordinalAssociations.clear();
    return setNextDoc(docID) ? ordinalAssociations : null;
  }
  
}
