package org.apache.lucene.facet.search.aggregator.association;

import java.io.IOException;

import org.apache.lucene.facet.enhancements.association.AssociationsPayloadIterator;
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
 * An {@link Aggregator} which updates the weight of a category by summing the
 * weights of the float association it finds for every document.
 * 
 * @lucene.experimental
 */
public class AssociationFloatSumAggregator implements Aggregator {

  protected final String field;
  protected final float[] sumArray;
  protected final AssociationsPayloadIterator associationsPayloadIterator;

  public AssociationFloatSumAggregator(IndexReader reader, float[] sumArray) throws IOException {
    this(CategoryListParams.DEFAULT_TERM.field(), reader, sumArray);
  }
  
  public AssociationFloatSumAggregator(String field, IndexReader reader, float[] sumArray) throws IOException {
    this.field = field;
    associationsPayloadIterator = new AssociationsPayloadIterator(reader, field);
    this.sumArray = sumArray;
  }

  public void aggregate(int ordinal) {
    long association = associationsPayloadIterator.getAssociation(ordinal);
    if (association != AssociationsPayloadIterator.NO_ASSOCIATION) {
      sumArray[ordinal] += Float.intBitsToFloat((int) association);
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

  public void setNextDoc(int docid, float score) throws IOException {
    associationsPayloadIterator.setNextDoc(docid);
  }
  
}
