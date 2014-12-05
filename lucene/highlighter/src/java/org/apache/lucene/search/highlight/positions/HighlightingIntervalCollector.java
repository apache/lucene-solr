package org.apache.lucene.search.highlight.positions;
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

import org.apache.lucene.search.PositionsCollector;
import org.apache.lucene.search.posfilter.Interval;

/**
 * Collects the first maxDocs docs and their positions matching the query
 * 
 * @lucene.experimental
 */

public class HighlightingIntervalCollector extends PositionsCollector {
  
  int count;
  DocAndPositions docs[];
  
  public HighlightingIntervalCollector (int maxDocs) {
    super(true);
    docs = new DocAndPositions[maxDocs];
  }

  @Override
  protected void collectPosition(int doc, Interval interval) {
    if (count > docs.length)
      return;   // TODO can we indicate collection has finished somehow?
    if (count <= 0 || docs[count - 1].doc != doc) {
      DocAndPositions spdoc = new DocAndPositions(doc);
      docs[count++] = spdoc;
    }
    docs[count - 1].storePosition(interval);
  }
  
  public DocAndPositions[] getDocs () {
    DocAndPositions ret[] = new DocAndPositions[count];
    System.arraycopy(docs, 0, ret, 0, count);
    return ret;
  }

}
