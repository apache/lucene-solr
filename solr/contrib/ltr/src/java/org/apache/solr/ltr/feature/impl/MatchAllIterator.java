package org.apache.solr.ltr.feature.impl;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

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

public class MatchAllIterator extends DocIdSetIterator {
  protected int docID = -1;

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public int nextDoc() throws IOException {
    // only the rescorer will call this scorers and nextDoc will never be called
    return ++docID; // FIXME: Keep this or throw new
                    // UnsupportedOperationException()?
  }

  @Override
  public int advance(int target) throws IOException {
    // For advanced features that use Solr scorers internally, you must override
    // and pass this call on to them
    docID = target;
    return docID;
  }

  @Override
  public long cost() {
    return 0; // FIXME: Do something here
  }

}
