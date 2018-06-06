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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;

public class PassageCollector implements HighlightCollector {

  private final Set<String> fields;
  private final int maxPassagesPerField;
  private final Supplier<PassageBuilder> passageSource;
  private final Map<String, PassageBuilder> builders = new HashMap<>();

  private Matches matches;
  private final Map<String, MatchesIterator> iterators = new HashMap<>();

  public PassageCollector(Set<String> fields, int maxPassagesPerField, Supplier<PassageBuilder> passageSource) {
    this.fields = fields;
    this.maxPassagesPerField = maxPassagesPerField;
    this.passageSource = passageSource;
  }

  @Override
  public void setMatches(Matches matches) {
    this.matches = matches;
  }

  @Override
  public Document getHighlights() {
    Document document = new Document();
    for (Map.Entry<String, PassageBuilder> passages : builders.entrySet()) {
      for (String snippet : passages.getValue().getTopPassages(maxPassagesPerField)) {
        document.add(new TextField(passages.getKey(), snippet, Field.Store.YES));
      }
    }
    return document;
  }

  @Override
  public Set<String> requiredFields() {
    return fields;
  }

  @Override
  public void collectHighlights(String field, String text, int offset) throws IOException {

    MatchesIterator mi = iterators.get(field);
    if (mi == null) {
      mi = matches.getMatches(field);
      if (mi == null) {
        return;
      }
      iterators.put(field, mi);
      if (mi.next() == false) {
        return;
      }
    }
    PassageBuilder passageBuilder = builders.computeIfAbsent(field, t -> passageSource.get());
    passageBuilder.addSource(text);

    do {
      if (passageBuilder.addMatch(mi.startOffset() - offset, mi.endOffset() - offset) == false) {
        break;
      }
    } while (mi.next());

  }

}
