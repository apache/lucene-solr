package org.apache.lucene.server;

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

import java.text.BreakIterator;
import java.util.Locale;

import org.apache.lucene.search.postingshighlight.Passage;
import org.apache.lucene.search.postingshighlight.PassageFormatter;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

// nocommit move something like this to Lucene?

/** Formats highlight hits for single-valued fields,
 *  trimming snippets that are too large at word
 *  boundaries. */
public class SVJSONPassageFormatter extends PassageFormatter {
  private final int maxSnippetLength;
  private final BreakIterator wordBI = BreakIterator.getWordInstance(Locale.ROOT);

  /** Sole constructor. */
  public SVJSONPassageFormatter(int maxSnippetLength) {
    this.maxSnippetLength = maxSnippetLength;
  }

  @Override
  public Object format(Passage[] passages, String content) {
    JSONArray result = new JSONArray();

    int pos = 0;
    JSONArray snippet = null;

    Passage lastPassage = null;
    JSONObject lastSnippetObj = null;

    int totLength = 0;

    for (Passage passage : passages) {
      if (snippet == null || passage.getStartOffset() > pos) {
        if (lastPassage != null) {
          lastSnippetObj.put("endOffset", lastPassage.getEndOffset());
        }

        // Make a new snippet
        JSONObject snippetObj = new JSONObject();
        lastSnippetObj = snippetObj;
        snippetObj.put("startOffset", passage.getStartOffset());
        snippet = new JSONArray();
        snippetObj.put("parts", snippet);
        result.add(snippetObj);
        pos = passage.getStartOffset();
        totLength = 0;
      }

      // TODO: make this 4 settable
      int limit = Math.min(4, passage.getNumMatches());
      
      for (int i = 0; i < limit; i++) {
        int start = passage.getMatchStarts()[i];
        // Must at least start within passage:
        assert start < passage.getEndOffset();
        int end = passage.getMatchEnds()[i];
        // it's possible to have overlapping terms
        if (start > pos) {
          String s = trim(content.substring(pos, start), i>0, true);
          totLength += s.length();
          snippet.add(s);
          pos = start;
        }
        if (end > pos) {
          JSONObject hit = new JSONObject();
          snippet.add(hit);
          hit.put("term", passage.getMatchTerms()[i].utf8ToString());
          String s = content.substring(Math.max(pos, start), end);
          totLength += s.length();
          hit.put("text", s);
          pos = end;
        }
        // TODO: make this 3*maxSnippetLength settable
        if (totLength > 3*maxSnippetLength) {
          break;
        }
      }
      if (totLength < 3*maxSnippetLength) {
        // its possible a "term" from the analyzer could span a sentence boundary.
        snippet.add(trim(content.substring(pos, Math.max(pos, passage.getEndOffset())), passage.getNumMatches() != 0, false));
      }
      pos = passage.getEndOffset();
      lastPassage = passage;
    }

    if (lastPassage != null) {
      lastSnippetObj.put("endOffset", lastPassage.getEndOffset());
    }

    return result;
  }

  /** Find last word boundary before offset. */
  private int wordBack(int offset) {
    int x = wordBI.preceding(offset);
    if (x < offset-15) {
      x = offset;
    }
    if (x < 0) {
      x = 0;
    }
    return x;
  }

  /** Find next word boundary after offset. */
  private int wordForwards(int offset) {
    int x = wordBI.following(offset);
    if (x > offset+15) {
      x = offset;
    }
    return x;
  }

  private String trim(String in, boolean hasMatchBeforeStart, boolean hasMatchAfterEnd) {
    if (in.length() <= maxSnippetLength) {
      return in;
    }

    wordBI.setText(in);

    if (hasMatchBeforeStart) {
      if (hasMatchAfterEnd) {
        if (in.length() <= 2*maxSnippetLength) {
          return in;
        } else {
          return in.substring(0, wordBack(maxSnippetLength)) + " ... " + in.substring(wordForwards(in.length()-maxSnippetLength));
        }
      } else {
        return in.substring(0, wordBack(maxSnippetLength)) + " ... ";
      }
    } else if (hasMatchAfterEnd) {
      return " ... " + in.substring(wordForwards(in.length()-maxSnippetLength));
    } else {
      return in.substring(0, wordBack(maxSnippetLength)) + " ... ";
    }
  }
}
