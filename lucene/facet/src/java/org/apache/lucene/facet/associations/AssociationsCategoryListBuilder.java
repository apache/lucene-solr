package org.apache.lucene.facet.associations;

import java.io.IOException;
import java.util.HashMap;

import org.apache.lucene.facet.index.CategoryListBuilder;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.store.ByteArrayDataOutput;
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
 * A {@link CategoryListBuilder} which encodes category-association value pairs
 * in addition to regular counting list category ordinals. Every
 * category-association pair is written under the respective association's
 * {@link CategoryAssociation#getCategoryListID()}.
 */
public class AssociationsCategoryListBuilder extends CategoryListBuilder {
 
  private final CategoryAssociationsContainer associations;
  private final HashMap<String,BytesRef> perAssociationBytes = new HashMap<String,BytesRef>();
  private final ByteArrayDataOutput output = new ByteArrayDataOutput();
  
  public AssociationsCategoryListBuilder(CategoryAssociationsContainer associations, 
      CategoryListParams categoryListParams, FacetIndexingParams indexingParams, TaxonomyWriter taxoWriter) {
    super(categoryListParams, indexingParams, taxoWriter);
    this.associations = associations;
  }
  
  @Override
  public void handle(int ordinal, CategoryPath cp) throws IOException {
    super.handle(ordinal, cp);
    
    // build per-association key BytesRef
    CategoryAssociation association = associations.getAssociation(cp);
    if (association == null) {
      // it is ok to set a null association for a category - it's treated as a
      // regular category in that case.
      return;
    }
    
    BytesRef bytes = perAssociationBytes.get(association.getCategoryListID());
    if (bytes == null) {
      bytes = new BytesRef();
      perAssociationBytes.put(association.getCategoryListID(), bytes);
    }
    
    int maxBytesNeeded = 4 /* int */ + association.maxBytesNeeded();
    if (bytes.bytes.length - bytes.length < maxBytesNeeded) {
      bytes.grow(bytes.bytes.length + maxBytesNeeded);
    }
    
    // reset the output to write from bytes.length (current position) until the end
    output.reset(bytes.bytes, bytes.length, bytes.bytes.length - bytes.length);
    output.writeInt(ordinal);
    
    // encode the association bytes
    association.serialize(output);
    
    // update BytesRef
    bytes.length = output.getPosition();
  }

  @Override
  public HashMap<String,BytesRef> finish() {
    // build the ordinals list
    HashMap<String,BytesRef> result = super.finish();
    // add per association bytes
    result.putAll(perAssociationBytes);
    return result;
  }
  
}
