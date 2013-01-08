package org.apache.lucene.facet.search.aggregator.associations;

import java.io.IOException;

import org.apache.lucene.facet.associations.CategoryFloatAssociation;
import org.apache.lucene.facet.associations.FloatAssociationsPayloadIterator;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.search.aggregator.Aggregator;
import org.apache.lucene.index.IndexReader;

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
 * An {@link Aggregator} which computes the weight of a category as the sum of
 * the float values associated with it in the result documents.
 * 
 * @lucene.experimental
 */
public class AssociationFloatSumAggregator implements Aggregator {

  protected final String field;
  protected final float[] sumArray;
  protected final FloatAssociationsPayloadIterator associations;

  public AssociationFloatSumAggregator(IndexReader reader, float[] sumArray) throws IOException {
    this(CategoryListParams.DEFAULT_TERM.field(), reader, sumArray);
  }
  
  public AssociationFloatSumAggregator(String field, IndexReader reader, float[] sumArray) throws IOException {
    this.field = field;
    associations = new FloatAssociationsPayloadIterator(reader, field, new CategoryFloatAssociation());
    this.sumArray = sumArray;
  }

  @Override
  public void aggregate(int ordinal) {
    float association = associations.getAssociation(ordinal);
    if (!Float.isNaN(association)) {
      sumArray[ordinal] += association;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    AssociationFloatSumAggregator that = (AssociationFloatSumAggregator) obj;
    return that.field.equals(field) && that.sumArray == sumArray;
  }

  @Override
  public int hashCode() {
    return field.hashCode();
  }

  @Override
  public void setNextDoc(int docid, float score) throws IOException {
    associations.setNextDoc(docid);
  }
  
}
