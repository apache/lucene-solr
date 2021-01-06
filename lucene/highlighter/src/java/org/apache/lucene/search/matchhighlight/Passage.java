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
package org.apache.lucene.search.matchhighlight;

import java.util.List;

/**
 * A passage is a fragment of source text, scored and possibly with a list of sub-offsets (markers)
 * to be highlighted. The markers can be overlapping or nested, but they're always contained within
 * the passage.
 */
public class Passage extends OffsetRange {
  public List<OffsetRange> markers;

  public Passage(int from, int to, List<OffsetRange> markers) {
    super(from, to);

    this.markers = markers;
  }

  /** Passages can't be sliced as it could split previously determined highlight markers. */
  @Override
  public OffsetRange slice(int from, int to) {
    throw new RuntimeException("Passages.slice() does not make sense?");
  }

  @Override
  public String toString() {
    return "[" + super.toString() + ", markers=" + markers + "]";
  }
}
