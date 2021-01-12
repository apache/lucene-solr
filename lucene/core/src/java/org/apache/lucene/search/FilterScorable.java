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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Filter a {@link Scorable}, intercepting methods and optionally changing
 * their return values
 *
 * The default implementation simply passes all calls to its delegate, with
 * the exception of {@link #setMinCompetitiveScore(float)} which defaults
 * to a no-op.
 */
public class FilterScorable extends Scorable {

  protected final Scorable in;

  /**
   * Filter a scorer
   * @param in  the scorer to filter
   */
  public FilterScorable(Scorable in) {
    this.in = in;
  }

  @Override
  public float score() throws IOException {
    return in.score();
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public Collection<ChildScorable> getChildren() throws IOException {
    return Collections.singletonList(new ChildScorable(in, "FILTER"));
  }
}
