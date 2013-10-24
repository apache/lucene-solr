package org.apache.lucene.facet.associations;

import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.facet.index.DrillDownStream;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
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
 * A {@link DrillDownStream} which adds to each drill-down token a
 * payload according to the {@link CategoryAssociation} defined in the
 * {@link CategoryAssociationsContainer}.
 * 
 * @lucene.experimental
 */
public class AssociationsDrillDownStream extends DrillDownStream {

  private final PayloadAttribute payloadAttribute;
  private final BytesRef payload;
  private final ByteArrayDataOutput output = new ByteArrayDataOutput();
  private final CategoryAssociationsContainer associations;

  public AssociationsDrillDownStream(CategoryAssociationsContainer associations, FacetIndexingParams indexingParams) {
    super(associations, indexingParams);
    this.associations = associations;
    payloadAttribute = addAttribute(PayloadAttribute.class);
    BytesRef bytes = payloadAttribute.getPayload();
    if (bytes == null) {
      bytes = new BytesRef(new byte[4]);
      payloadAttribute.setPayload(bytes);
    }
    bytes.offset = 0;
    this.payload = bytes;
  }
  
  @Override
  protected void addAdditionalAttributes(CategoryPath cp, boolean isParent) {
    if (isParent) {
      return; // associations are not added to parent categories
    }
    
    CategoryAssociation association = associations.getAssociation(cp);
    if (association == null) {
      // it is ok to set a null association for a category - it's treated as a
      // regular category in that case.
      return;
    }
    if (payload.bytes.length < association.maxBytesNeeded()) {
      payload.grow(association.maxBytesNeeded());
    }
    output.reset(payload.bytes);
    association.serialize(output);
    payload.length = output.getPosition();
  }
  
}
