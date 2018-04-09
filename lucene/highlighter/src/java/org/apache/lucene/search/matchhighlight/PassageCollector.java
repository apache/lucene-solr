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
import java.text.BreakIterator;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.MatchesIterator;

public class PassageCollector implements SnippetCollector {

  private final Document document = new Document();

  private final Set<String> fields;

  public PassageCollector(Set<String> fields) {
    this.fields = fields;
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

    BreakIterator breakIterator = BreakIterator.getSentenceInstance(Locale.ROOT);
    breakIterator.setText(text); // nocommit is there a nicer way of doing this?

    StringBuilder snippet = new StringBuilder();
    int passageStart = breakIterator.first();
    int passageEnd = breakIterator.next();
    int snippetEnd = 0;
    MatchesIterator mi = matches.getMatches(field, text);

    while (mi.next()) {
      if (mi.startOffset() >= passageEnd) {
        // finish the current snippet and advance the BreakIterator until we're surrounding the current match
        if (snippet.length() > 0) {
          snippet.append(text.substring(snippetEnd, passageEnd));
          document.add(new TextField(field.name, snippet.toString(), Field.Store.YES));
          snippet = new StringBuilder();
        }
        while (passageEnd < mi.startOffset()) {
          passageStart = passageEnd;
          passageEnd = breakIterator.next();
        }
      }
      // append to the current snippet
      snippet.append(text.substring(passageStart, mi.startOffset()));
      snippet.append("<B>");  // TODO make configurable
      snippet.append(text.substring(mi.startOffset(), mi.endOffset()));
      snippet.append("</B>");
      passageStart = mi.endOffset() + 1;
    }

    if (snippet.length() > 0) {
      snippet.append(text.substring(snippetEnd, passageEnd));
      document.add(new TextField(field.name, snippet.toString(), Field.Store.YES));
    }
  }

}
