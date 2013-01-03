package org.apache.lucene.facet.associations;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
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

  public FloatAssociationsPayloadIterator(IndexReader reader, String field, CategoryFloatAssociation association) 
      throws IOException {
    super(reader, field, association);
  }

  @Override
  protected void handleAssociation(int ordinal, CategoryFloatAssociation association) {
    ordinalAssociations.put(ordinal, association.getValue());
  }
  
  @Override
  public boolean setNextDoc(int docId) throws IOException {
    ordinalAssociations.clear();
    return super.setNextDoc(docId);
  }

  /**
   * Get the float association value for the given ordinal, or
   * {@link Float#NaN} in case the ordinal has no association value.
   */
  public float getAssociation(int ordinal) {
    if (ordinalAssociations.containsKey(ordinal)) {
      return ordinalAssociations.get(ordinal);
    }

    return Float.NaN;
  }

}
