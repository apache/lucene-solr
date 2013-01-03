package org.apache.lucene.sandbox.postingshighlight;

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

/**
 * Creates a formatted snippet from the top passages.
 * <p>
 * The default implementation marks the query terms as bold, and places
 * ellipses between unconnected passages.
 * @lucene.experimental
 */
public class PassageFormatter {
  /**
   * Formats the top <code>passages</code> from <code>content</code>
   * into a human-readable text snippet.
   * 
   * @param passages top-N passages for the field. Note these are sorted in
   *        the order that they appear in the document for convenience.
   * @param content content for the field.
   * @return formatted highlight
   */
  public String format(Passage passages[], String content) {
    StringBuilder sb = new StringBuilder();
    int pos = 0;
    for (Passage passage : passages) {
      // don't add ellipsis if its the first one, or if its connected.
      if (passage.startOffset > pos && pos > 0) {
        sb.append("... ");
      }
      pos = passage.startOffset;
      for (int i = 0; i < passage.numMatches; i++) {
        int start = passage.matchStarts[i];
        int end = passage.matchEnds[i];
        // its possible to have overlapping terms
        if (start > pos) {
          sb.append(content.substring(pos, start));
        }
        if (end > pos) {
          sb.append("<b>");
          sb.append(content.substring(Math.max(pos, start), end));
          sb.append("</b>");
          pos = end;
        }
      }
      // its possible a "term" from the analyzer could span a sentence boundary.
      sb.append(content.substring(pos, Math.max(pos, passage.endOffset)));
      pos = passage.endOffset;
    }
    return sb.toString();
  }
}
