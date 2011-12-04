/**
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
package org.apache.lucene.spatial.tier;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class CartesianShapeFilter extends Filter {
 
  private final Shape shape;
  private final String fieldName;
  
  CartesianShapeFilter(final Shape shape, final String fieldName){
    this.shape = shape;
    this.fieldName = fieldName;
  }
  
  @Override
  public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    final List<Double> area = shape.getArea();
    final int sz = area.size();
    
    // iterate through each boxid
    final BytesRef bytesRef = new BytesRef(NumericUtils.BUF_SIZE_LONG);
    if (sz == 1) {
      double boxId = area.get(0).doubleValue();
      NumericUtils.longToPrefixCoded(NumericUtils.doubleToSortableLong(boxId), 0, bytesRef);
      return new DocIdSet() {
        @Override
        public DocIdSetIterator iterator() throws IOException {
          return context.reader.termDocsEnum(acceptDocs, fieldName, bytesRef, false);
        }
        
        @Override
        public boolean isCacheable() {
          return false;
        }
      };
    } else {
      final FixedBitSet bits = new FixedBitSet(context.reader.maxDoc());
      for (int i =0; i< sz; i++) {
        double boxId = area.get(i).doubleValue();
        NumericUtils.longToPrefixCoded(NumericUtils.doubleToSortableLong(boxId), 0, bytesRef);
        final DocsEnum docsEnum = context.reader.termDocsEnum(acceptDocs, fieldName, bytesRef, false);
        if (docsEnum == null) continue;
        // iterate through all documents
        // which have this boxId
        int doc;
        while ((doc = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          bits.set(doc);
        }
      }
      return bits;
    }
  }
}
