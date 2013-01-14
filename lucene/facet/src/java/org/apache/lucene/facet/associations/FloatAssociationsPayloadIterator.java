package org.apache.lucene.facet.associations;

import java.io.IOException;

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
 * An {@link AssociationsPayloadIterator} over integer association values.
 * 
 * @lucene.experimental
 */
public class FloatAssociationsPayloadIterator extends AssociationsPayloadIterator<CategoryFloatAssociation> {

  private final IntToFloatMap ordinalAssociations = new IntToFloatMap();

  public FloatAssociationsPayloadIterator(String field, CategoryFloatAssociation association) throws IOException {
    super(field, association);
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
