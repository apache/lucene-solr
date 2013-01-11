package org.apache.lucene.facet.associations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.facet.index.CategoryListBuilder;
import org.apache.lucene.facet.index.CountingListBuilder;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

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
 * A {@link AssociationsListBuilder} which encodes category-association value pairs.
 * Every category-association pair is written under the respective association's
 * {@link CategoryAssociation#getCategoryListID()}.
 * <p>
 * <b>NOTE:</b> associations list do not encode the counting list data. You
 * should use {@link CountingListBuilder} to build that information and then
 * merge the results of both {@link #build(IntsRef, Iterable)}.
 */
public class AssociationsListBuilder implements CategoryListBuilder {
  
  private final CategoryAssociationsContainer associations;
  private final ByteArrayDataOutput output = new ByteArrayDataOutput();
  
  public AssociationsListBuilder(CategoryAssociationsContainer associations) {
    this.associations = associations;
  }
  
  @Override
  public Map<String,BytesRef> build(IntsRef ordinals, Iterable<CategoryPath> categories) throws IOException {
    final HashMap<String,BytesRef> res = new HashMap<String,BytesRef>();
    int idx = 0;
    for (CategoryPath cp : categories) {
      // build per-association key BytesRef
      CategoryAssociation association = associations.getAssociation(cp);
      
      if (association == null) {
        // it is ok to set a null association for a category - it's treated as a
        // regular category in that case.
        ++idx;
        continue;
      }

      BytesRef bytes = res.get(association.getCategoryListID());
      if (bytes == null) {
        bytes = new BytesRef(32);
        res.put(association.getCategoryListID(), bytes);
      }
      
      int maxBytesNeeded = 4 /* int */ + association.maxBytesNeeded() + bytes.length;
      if (bytes.bytes.length < maxBytesNeeded) {
        bytes.grow(maxBytesNeeded);
      }
      
      // reset the output to write from bytes.length (current position) until the end
      output.reset(bytes.bytes, bytes.length, bytes.bytes.length - bytes.length);
      output.writeInt(ordinals.ints[idx++]);
      
      // encode the association bytes
      association.serialize(output);
      
      // update BytesRef
      bytes.length = output.getPosition();
    }

    return res;
  }
  
}
