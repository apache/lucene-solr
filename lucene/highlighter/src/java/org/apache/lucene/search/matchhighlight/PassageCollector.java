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
import java.util.Set;
import java.util.function.Function;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.MatchesIterator;

public class PassageCollector implements SnippetCollector {

  private final Document document = new Document();

  private final Set<String> fields;
  private final int maxPassagesPerField;
  private final Function<String, PassageBuilder> passageSource;

  public PassageCollector(Set<String> fields, int maxPassagesPerField, Function<String, PassageBuilder> passageSource) {
    this.fields = fields;
    this.maxPassagesPerField = maxPassagesPerField;
    this.passageSource = passageSource;
  }

  @Override
  public Document getHighlights() {
    return document;
  }

  @Override
  public boolean needsField(String name) {
    return fields.contains(name);
  }

  @Override
  public void collectSnippets(SourceAwareMatches matches, FieldInfo field, String text) throws IOException {

    MatchesIterator mi = matches.getMatches(field, text);
    PassageBuilder passageBuilder = passageSource.apply(text);
    while (mi.next()) {
      passageBuilder.addMatch(mi.term(), mi.startOffset(), mi.endOffset());
    }

    for (String snippet : passageBuilder.getTopPassages(maxPassagesPerField)) {
      document.add(new TextField(field.name, snippet, Field.Store.YES));
    }

  }

}
