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

import org.apache.lucene.search.postingshighlight.Passage;
import org.apache.lucene.search.postingshighlight.PassageFormatter;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/** From a multi-valued field (indexed with the values
 *  joined with INFO_SEP), highlight each value entirely. */
public class WholeMVJSONPassageFormatter extends PassageFormatter {

  private final int offsetGap;

  /** Create this, with the specified offsetGap. */
  public WholeMVJSONPassageFormatter(int offsetGap) {
    this.offsetGap = offsetGap;
  }

  /** Carefully finds the field boundaries
   *  (INFORMATION_SEPARATOR) in the content and builds a
   *  JSONArray so that each original field value is
   *  separated and highlighted. */
  @Override
  public JSONArray format(Passage[] passages, String content) {

    // Caller must use WholeBreakIterator:
    assert passages.length == 1;
    Passage passage = passages[0];

    String[] chunks = content.split(Constants.INFORMATION_SEP_REGEX);
    JSONArray result = new JSONArray();
    int matchUpto = 0;
    int charOffset = 0;
    for(String chunk : chunks) {
      JSONArray part = new JSONArray();
      result.add(part);
      int pos = 0;
      int posEnd = chunk.length();
      while (matchUpto < passage.getNumMatches()) {
        int start = passage.getMatchStarts()[matchUpto] - charOffset;
        if (start >= posEnd) {
          break;
        }
        if (start > pos) {
          part.add(chunk.substring(pos, start));
          pos = start;
        }
        JSONObject match = new JSONObject();
        part.add(match);
        int end = passage.getMatchEnds()[matchUpto] - charOffset;
        match.put("text", chunk.substring(start, end));
        match.put("term", passage.getMatchTerms()[matchUpto].utf8ToString());
        pos = end;
        matchUpto++;
      }
      if (pos < chunk.length()) {
        part.add(chunk.substring(pos));
        pos = chunk.length();
      }

      // nocommit we always join w/ INFO_SEP ... so it
      //should just be 1?
      //charOffset += chunk.length()+offsetGap;
      charOffset += chunk.length()+1;
    }

    return result;
  }
}
