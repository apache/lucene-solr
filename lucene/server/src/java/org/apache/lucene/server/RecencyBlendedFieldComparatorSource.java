package org.apache.lucene.server;

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

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;

/** Sorts by score blended with recency, by looking at a
 *  separate timestamp field. */

public class RecencyBlendedFieldComparatorSource extends FieldComparatorSource {
  final String timeStampFieldName;
  final float maxBoost;
  final long range;
  final long currentTime;

  public RecencyBlendedFieldComparatorSource(String timeStampFieldName, float maxBoost, long currentTime, long range) {
    this.timeStampFieldName = timeStampFieldName;
    this.maxBoost = maxBoost;
    this.range = range;
    this.currentTime = currentTime;
  }

  @Override
  public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
    return new BlendedComparator(numHits) {

      NumericDocValues dv;

      @Override
      public FieldComparator<Float> setNextReader(AtomicReaderContext context) throws IOException {
        dv = context.reader().getNumericDocValues(timeStampFieldName);
        if (dv == null) {
          // This can happen if app fails to add the dv field:
          throw new IllegalStateException("reader=" + context.reader() + " does not have numeric doc values for field=" + timeStampFieldName);
        }

        return this;
      }

      @Override
      protected float getScore(int doc) {
  
        // Relevance:
        float score;
        try {
          score = scorer.score();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }

        long age = currentTime - dv.get(doc);
        double boost;

        if (age > range) {
          boost = 1.0;
        } else {
          // Simple linear boost:
          boost = maxBoost * (((double) (range - age)) / range);
        }

        //System.out.println("doc=" + doc + " score=" + score + " boost=" + boost);

        return (float) boost * score;
      }
    };
  }
}
