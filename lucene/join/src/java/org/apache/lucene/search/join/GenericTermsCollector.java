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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.join.DocValuesTermsCollector.Function;
import org.apache.lucene.search.join.TermsWithScoreCollector.MV;
import org.apache.lucene.search.join.TermsWithScoreCollector.SV;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

interface GenericTermsCollector extends Collector {
  
  BytesRefHash getCollectedTerms() ;
  
  float[] getScoresPerTerm();
  
  static GenericTermsCollector createCollectorMV(Function<SortedSetDocValues> mvFunction,
      ScoreMode mode) {
    
    switch (mode) {
      case None:
        return wrap(new TermsCollector.MV(mvFunction));
      case Avg:
          return new MV.Avg(mvFunction);
      default:
          return new MV(mvFunction, mode);
    }
  }

  static Function<SortedSetDocValues> verbose(PrintStream out, Function<SortedSetDocValues> mvFunction){
    return (ctx) -> {
      final SortedSetDocValues target = mvFunction.apply(ctx);
      return new SortedSetDocValues() {
        
        @Override
        public void setDocument(int docID) {
          target.setDocument(docID);
          out.println("\ndoc# "+docID);
        }
        
        @Override
        public long nextOrd() {
          return target.nextOrd();
        }
        
        @Override
        public BytesRef lookupOrd(long ord) {
          final BytesRef val = target.lookupOrd(ord);
          out.println(val.toString()+", ");
          return val;
        }
        
        @Override
        public long getValueCount() {
          return target.getValueCount();
        }
      };
      
    };
  }

  static GenericTermsCollector createCollectorSV(Function<BinaryDocValues> svFunction,
      ScoreMode mode) {
    
    switch (mode) {
      case None:
        return wrap(new TermsCollector.SV(svFunction));
      case Avg:
        return new SV.Avg(svFunction);
      default:
        return new SV(svFunction, mode);  
    }
  }
  
  static GenericTermsCollector wrap(final TermsCollector<?> collector) {
    return new GenericTermsCollector() {

      
      @Override
      public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return collector.getLeafCollector(context);
      }

      @Override
      public boolean needsScores() {
        return collector.needsScores();
      }

      @Override
      public BytesRefHash getCollectedTerms() {
        return collector.getCollectorTerms();
      }

      @Override
      public float[] getScoresPerTerm() {
        throw new UnsupportedOperationException("scores are not available for "+collector);
      }
    };
  }
}
