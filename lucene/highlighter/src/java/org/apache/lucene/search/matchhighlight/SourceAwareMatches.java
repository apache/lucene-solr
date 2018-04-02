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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;

class SourceAwareMatches {

  private final Matches in;
  private final Analyzer analyzer;

  private final Map<FieldInfo, SourceAwareMatchesIterator> iterators = new HashMap<>();

  SourceAwareMatches(Matches in, Analyzer analyzer) {
    this.in = in;
    this.analyzer = analyzer;
  }

  MatchesIterator getMatches(FieldInfo fi, byte[] value) {
    SourceAwareMatchesIterator it = iterators.computeIfAbsent(fi, fieldinfo -> {
      if (fieldinfo.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
        return SourceAwareMatchesIterator.wrapOffsets(in.getMatches(fieldinfo.name));
      return SourceAwareMatchesIterator.fromTokenStream(in.getMatches(fieldinfo.name), analyzer);
    });
    it.addSource(value);
    return it;
  }
}
