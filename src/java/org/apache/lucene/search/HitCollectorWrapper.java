package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

/**
 * Wrapper for ({@link HitCollector}) implementations, which simply re-bases the
 * incoming docID before calling {@link HitCollector#collect}.
 * 
 * @deprecated Please migrate custom HitCollectors to the new {@link Collector}
 *             class. This class will be removed when {@link HitCollector} is
 *             removed.
 */
public class HitCollectorWrapper extends Collector {
  private HitCollector collector;
  private int base = 0;
  private Scorer scorer = null;
  
  public HitCollectorWrapper(HitCollector collector) {
    this.collector = collector;
  }
  
  public void setNextReader(IndexReader reader, int docBase) {
    base = docBase;
  }

  public void collect(int doc) throws IOException {
    collector.collect(doc + base, scorer.score());
  }

  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;      
  }
  
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

}
