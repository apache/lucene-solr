package org.apache.lucene.facet.associations;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
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
 * An {@link AssociationsPayloadIterator} over integer association values.
 * 
 * @lucene.experimental
 */
public class IntAssociationsPayloadIterator extends AssociationsPayloadIterator<CategoryIntAssociation> {

  private final IntToIntMap ordinalAssociations = new IntToIntMap();

  /**
   * The long-special-value returned for ordinals which have no associated int
   * value. It is not in the int range of values making it a valid mark.
   */
  public final static long NO_ASSOCIATION = Integer.MAX_VALUE + 1;

  public IntAssociationsPayloadIterator(IndexReader reader, String field, CategoryIntAssociation association) 
      throws IOException {
    super(reader, field, association);
  }

  @Override
  protected void handleAssociation(int ordinal, CategoryIntAssociation association) {
    ordinalAssociations.put(ordinal, association.getValue());
  }
  
  @Override
  public boolean setNextDoc(int docId) throws IOException {
    ordinalAssociations.clear();
    return super.setNextDoc(docId);
  }

  /**
   * Get the integer association value for the given ordinal, or
   * {@link #NO_ASSOCIATION} in case the ordinal has no association value.
   */
  public long getAssociation(int ordinal) {
    if (ordinalAssociations.containsKey(ordinal)) {
      return ordinalAssociations.get(ordinal);
    }

    return NO_ASSOCIATION;
  }

}
