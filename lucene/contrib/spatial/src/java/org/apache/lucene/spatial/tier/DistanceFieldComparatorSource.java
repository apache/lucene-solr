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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;

/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class DistanceFieldComparatorSource extends FieldComparatorSource {

  private DistanceFilter distanceFilter;
  private DistanceScoreDocLookupComparator dsdlc;

  public DistanceFieldComparatorSource(Filter distanceFilter) {
    this.distanceFilter = (DistanceFilter) distanceFilter;
  }

  public void cleanUp() {
    distanceFilter = null;

    if (dsdlc != null) {
      dsdlc.cleanUp();
    }

    dsdlc = null;
  }

  @Override
  public FieldComparator newComparator(String fieldname, int numHits,
                                         int sortPos, boolean reversed) throws IOException {
    dsdlc = new DistanceScoreDocLookupComparator(numHits);
    return dsdlc;
  }

  private class DistanceScoreDocLookupComparator extends FieldComparator<Double> {

    private double[] values;
    private double bottom;
    private int offset =0;
		
    public DistanceScoreDocLookupComparator(int numHits) {
      values = new double[numHits];
      return;
    }

    @Override
    public int compare(int slot1, int slot2) {
      double a = values[slot1];
      double b = values[slot2];
      if (a > b)
        return 1;
      if (a < b)
        return -1;

      return 0;
    }

    public void cleanUp() {
      distanceFilter = null;
    }

    @Override
    public int compareBottom(int doc) {
      double v2 = distanceFilter.getDistance(doc+ offset);
			
      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      }
      return 0;
    }

    @Override
    public void copy(int slot, int doc) {
      values[slot] = distanceFilter.getDistance(doc + offset);
    }

    @Override
    public void setBottom(int slot) {
      this.bottom = values[slot];
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context)
      throws IOException {
      // each reader in a segmented base
      // has an offset based on the maxDocs of previous readers
      offset = context.docBase;
      return this;
    }

    @Override
    public Double value(int slot) {
      return values[slot];
    }
  }
}
