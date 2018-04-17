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
import org.apache.lucene.index.FieldInfo;

public class PassageCollector implements HighlightCollector {

  private final Set<String> fields;
  private final int maxPassagesPerField;
  private final Supplier<PassageBuilder> passageSource;
  private final Map<String, PassageBuilder> builders = new HashMap<>();

  public PassageCollector(Set<String> fields, int maxPassagesPerField, Supplier<PassageBuilder> passageSource) {
    this.fields = fields;
    this.maxPassagesPerField = maxPassagesPerField;
    this.passageSource = passageSource;
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
  public boolean needsField(String name) {
    return fields.contains(name);
  }

  @Override
  public void collectHighlights(SourceAwareMatches matches, FieldInfo field, String text) throws IOException {

    SourceAwareMatchesIterator mi = matches.getMatches(field, text);
    PassageBuilder passageBuilder = builders.computeIfAbsent(field.name, t -> passageSource.get());
    passageBuilder.addSource(text);

    while (mi.next()) {
      if (passageBuilder.addMatch(mi.term(), mi.startOffset(), mi.endOffset()) == false) {
        break;
      }
    }

  }

}
