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

package org.apache.solr.search;

import org.apache.lucene.search.HitCollector;
import org.apache.solr.util.OpenBitSet;
import org.apache.solr.core.SolrConfig;

/**
 * @version $Id$
 */

final class DocSetHitCollector extends HitCollector {

  final float HASHSET_INVERSE_LOAD_FACTOR;
  final int HASHDOCSET_MAXSIZE;

  int pos=0;
  OpenBitSet bits;
  final int maxDoc;

  // in case there aren't that many hits, we may not want a very sparse
  // bit array.  Optimistically collect the first few docs in an array
  // in case there are only a few.
  final int[] scratch;

  // todo - could pass in bitset and an operation also...
  DocSetHitCollector(float inverseLoadFactor, int maxSize, int maxDoc) {
    this.maxDoc = maxDoc;
    HASHSET_INVERSE_LOAD_FACTOR = inverseLoadFactor;
    HASHDOCSET_MAXSIZE = maxSize;
    scratch = new int[HASHDOCSET_MAXSIZE];
  }

  public void collect(int doc, float score) {
    // optimistically collect the first docs in an array
    // in case the total number will be small enough to represent
    // as a HashDocSet() instead...
    // Storing in this array will be quicker to convert
    // than scanning through a potentially huge bit vector.
    // FUTURE: when search methods all start returning docs in order, maybe
    // we could have a ListDocSet() and use the collected array directly.
    if (pos < scratch.length) {
      scratch[pos]=doc;
    } else {
      // this conditional could be removed if BitSet was preallocated, but that
      // would take up more memory, and add more GC time...
      if (bits==null) bits = new OpenBitSet(maxDoc);
      bits.fastSet(doc);
    }

    pos++;
  }

  public DocSet getDocSet() {
    if (pos<=scratch.length) {
      return new HashDocSet(scratch,0,pos,HASHSET_INVERSE_LOAD_FACTOR);
    } else {
      // set the bits for ids that were collected in the array
      for (int i=0; i<scratch.length; i++) bits.fastSet(scratch[i]);
      return new BitDocSet(bits,pos);
    }
  }
}
