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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.OpenBitSet;

/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class CartesianShapeFilter extends Filter {

  private static final Logger log = Logger.getLogger(CartesianShapeFilter.class.getName());

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private Shape shape;
  private String fieldName;
  
  CartesianShapeFilter(Shape shape, String fieldName){
    this.shape = shape;
    this.fieldName = fieldName;
  }
  
  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    long start = System.currentTimeMillis();
      
    OpenBitSet bits = new OpenBitSet(reader.maxDoc());

    TermDocs termDocs = reader.termDocs();
    List<Double> area = shape.getArea();
    int sz = area.size();
    log.fine("Area size "+ sz);

    // iterate through each boxid
    for (int i =0; i< sz; i++) {
      double boxId = area.get(i).doubleValue();
      termDocs.seek(new Term(fieldName,
          NumericUtils.doubleToPrefixCoded(boxId)));
      
      // iterate through all documents
      // which have this boxId
      while (termDocs.next()) {
        bits.fastSet(termDocs.doc());
      }
    }
    
    long end = System.currentTimeMillis();
    if(log.isLoggable(Level.FINE)) {
      log.fine("BoundaryBox Time Taken: "+ (end - start) + " found: "+bits.cardinality()+" candidates");
    }
    return bits;
  }
}
