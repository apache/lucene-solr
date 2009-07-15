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

package org.apache.solr.search.function;

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;

/** Internal class, subject to change.
 *  Serves as base class for DocValues based on StringIndex 
 **/
public abstract class StringIndexDocValues extends DocValues {
    protected final FieldCache.StringIndex index;
    protected final int[] order;
    protected final String[] lookup;
    protected final ValueSource vs;

    public StringIndexDocValues(ValueSource vs, IndexReader reader, String field) throws IOException {
      index = FieldCache.DEFAULT.getStringIndex(reader, field);
      order = index.order;
      lookup = index.lookup;
      this.vs = vs;
    }

    protected abstract String toTerm(String readableValue);

   @Override
    public ValueSourceScorer getRangeScorer(IndexReader reader, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
      // TODO: are lowerVal and upperVal in indexed form or not?
      lowerVal = lowerVal == null ? null : toTerm(lowerVal);
      upperVal = upperVal == null ? null : toTerm(upperVal);

     int lower = Integer.MIN_VALUE;
     if (lowerVal != null) {
       lower = index.binarySearchLookup(lowerVal);
       if (lower < 0) {
         lower = -lower-1;
       } else if (!includeLower) {
         lower++;
       }
     }

     int upper = Integer.MAX_VALUE;
     if (upperVal != null) {
       upper = index.binarySearchLookup(upperVal);
       if (upper < 0) {
         upper = -upper-2;
       } else if (!includeUpper) {
         upper--;
       }
     }

      final int ll = lower;
      final int uu = upper;

      return new ValueSourceScorer(reader, this) {
        @Override
        public boolean matchesValue(int doc) {
          int ord = order[doc];
          return ord >= ll && ord <= uu;
        }
      };
    }

    public String toString(int doc) {
      return vs.description() + '=' + strVal(doc);
    }

  }
