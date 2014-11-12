package org.apache.lucene.search.posfilter;

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

import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;

public class WithinFilteredScorer extends PositionFilteredScorer {

  private final int slop;
  private final PositionFilteredScorer wrappedScorer;

  public WithinFilteredScorer(PositionFilteredScorer wrappedScorer, int slop, Similarity.SimScorer simScorer) {
    super(wrappedScorer, simScorer);
    this.slop = slop;
    this.wrappedScorer = wrappedScorer;
  }

  @Override
  protected int doNextPosition() throws IOException {
    int position;
    while ((position = wrappedScorer.nextPosition()) != NO_MORE_POSITIONS) {
      if (wrappedScorer.getMatchDistance() <= slop) {
        current.update(wrappedScorer);
        return position;
      }
    }
    return NO_MORE_POSITIONS;
  }

}
