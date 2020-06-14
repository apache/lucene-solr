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
package org.apache.solr.client.solrj.response;

import org.apache.solr.common.util.NamedList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A base class for all analysis responses.
 *
 *
 * @since solr 1.4
 */
public class AnalysisResponseBase extends SolrResponseBase {

  /**
   * Parses the given named list and builds a list of analysis phases form it. Expects a named list of the form:
   * <br>
   * <pre><code>
   *  &lt;lst name="index"&gt;
   *      &lt;arr name="Tokenizer"&gt;
   *          &lt;str name="text"&gt;the_text&lt;/str&gt;
   *          &lt;str name="rawText"&gt;the_raw_text&lt;/str&gt; (optional)
   *          &lt;str name="type"&gt;the_type&lt;/str&gt;
   *          &lt;int name="start"&gt;1&lt;/str&gt;
   *          &lt;int name="end"&gt;3&lt;/str&gt;
   *          &lt;int name="position"&gt;1&lt;/str&gt;
   *          &lt;bool name="match"&gt;true | false&lt;/bool&gt; (optional)
   *      &lt;/arr&gt;
   *      &lt;arr name="Filter1"&gt;
   *          &lt;str name="text"&gt;the_text&lt;/str&gt;
   *          &lt;str name="rawText"&gt;the_raw_text&lt;/str&gt; (optional)
   *          &lt;str name="type"&gt;the_type&lt;/str&gt;
   *          &lt;int name="start"&gt;1&lt;/str&gt;
   *          &lt;int name="end"&gt;3&lt;/str&gt;
   *          &lt;int name="position"&gt;1&lt;/str&gt;
   *          &lt;bool name="match"&gt;true | false&lt;/bool&gt; (optional)
   *      &lt;/arr&gt;
   *      ...
   *  &lt;/lst&gt;
   * </code></pre>
   *
   * The special case is a CharacterFilter that just returns a string, which we then map to a single token without type.
   *
   * @param phaseNL The names list to parse.
   *
   * @return The built analysis phases list.
   */
  protected List<AnalysisPhase> buildPhases(NamedList<Object> phaseNL) {
    List<AnalysisPhase> phases = new ArrayList<>(phaseNL.size());
    for (Map.Entry<String, Object> phaseEntry : phaseNL) {
      AnalysisPhase phase = new AnalysisPhase(phaseEntry.getKey());
      Object phaseValue = phaseEntry.getValue();

      if (phaseValue instanceof String) {
        // We are looking at CharacterFilter, which - exceptionally - returns a string
        TokenInfo tokenInfo = buildTokenInfoFromString((String) phaseValue);
        phase.addTokenInfo(tokenInfo);
      } else {
        @SuppressWarnings({"unchecked"})
        List<NamedList<Object>> tokens = (List<NamedList<Object>>) phaseEntry.getValue();
        for (NamedList<Object> token : tokens) {
          TokenInfo tokenInfo = buildTokenInfo(token);
          phase.addTokenInfo(tokenInfo);
        }
      }
      phases.add(phase);
    }
    return phases;
  }

  /**
   * Convert a string value (from CharacterFilter) into a TokenInfo for its value full span.
   * @param value String value
   * @return The built token info (with type set to null)
   */
  protected TokenInfo buildTokenInfoFromString(String value) {
    return new TokenInfo(value, value, null, 0, value.length(), 1, false);
  }

  /**
   * Parses the given named list and builds a token infoform it. Expects a named list of the form:
   * <br>
   * <pre><code>
   *  &lt;arr name="Tokenizer"&gt;
   *      &lt;str name="text"&gt;the_text&lt;/str&gt;
   *      &lt;str name="rawText"&gt;the_raw_text&lt;/str&gt; (optional)
   *      &lt;str name="type"&gt;the_type&lt;/str&gt;
   *      &lt;int name="start"&gt;1&lt;/str&gt;
   *      &lt;int name="end"&gt;3&lt;/str&gt;
   *      &lt;int name="position"&gt;1&lt;/str&gt;
   *      &lt;bool name="match"&gt;true | false&lt;/bool&gt; (optional)
   *  &lt;/arr&gt;
   * </code></pre>
   *
   * @param tokenNL The named list to parse.
   *
   * @return The built token info.
   */
  protected TokenInfo buildTokenInfo(NamedList<Object> tokenNL) {
    String text = (String) tokenNL.get("text");
    String rawText = (String) tokenNL.get("rawText");
    String type = (String) tokenNL.get("type");
    int start = (Integer) tokenNL.get("start");
    int end = (Integer) tokenNL.get("end");
    int position = (Integer) tokenNL.get("position");
    Boolean match = (Boolean) tokenNL.get("match");
    return new TokenInfo(text, rawText, type, start, end, position, (match == null ? false : match));
  }


  //================================================= Inner Classes ==================================================

  /**
   * A phase in the analysis process. The phase holds the tokens produced in this phase and the name of the class that
   * produced them.
   */
  public static class AnalysisPhase {

    private final String className;
    private List<TokenInfo> tokens = new ArrayList<>();

    AnalysisPhase(String className) {
      this.className = className;
    }

    /**
     * The name of the class (analyzer, tokenzier, or filter) that produced the token stream for this phase.
     *
     * @return The name of the class that produced the token stream for this phase.
     */
    public String getClassName() {
      return className;
    }

    private void addTokenInfo(TokenInfo tokenInfo) {
      tokens.add(tokenInfo);
    }

    /**
     * Returns a list of tokens which represent the token stream produced in this phase.
     *
     * @return A list of tokens which represent the token stream produced in this phase.
     */
    public List<TokenInfo> getTokens() {
      return tokens;
    }

  }

  /**
   * Holds all information of a token as part of an analysis phase.
   */
  public static class TokenInfo {

    private final String text;
    private final String rawText;
    private final String type;
    private final int start;
    private final int end;
    private final int position;
    private final boolean match;

    /**
     * Constructs a new TokenInfo.
     *
     * @param text     The text of the token
     * @param rawText  The raw text of the token. If the token is stored in the index in a special format (e.g.
     *                 dates or padded numbers) this argument should hold this value. If the token is stored as is,
     *                 then this value should be {@code null}.
     * @param type     The type fo the token (typically either {@code word} or {@code <ALPHANUM>} though it depends
     *                 on the tokenizer/filter used).
     * @param start    The start position of the token in the original text where it was extracted from.
     * @param end      The end position of the token in the original text where it was extracted from.
     * @param position The position of the token within the token stream.
     * @param match    Indicates whether this token matches one of the the query tokens.
     */
    TokenInfo(String text, String rawText, String type, int start, int end, int position, boolean match) {
      this.text = text;
      this.rawText = rawText;
      this.type = type;
      this.start = start;
      this.end = end;
      this.position = position;
      this.match = match;
    }

    /**
     * Returns the text of the token.
     *
     * @return The text of the token.
     */
    public String getText() {
      return text;
    }

    /**
     * Returns the raw text of the token. If the token is index in a special format (e.g. date or paddded numbers)
     * it will be returned as the raw text. Returns {@code null} if the token is indexed as is.
     *
     * @return Returns the raw text of the token.
     */
    public String getRawText() {
      return rawText;
    }

    /**
     * Returns the type of the token. Typically this will be {@code word} or {@code <ALPHANUM>}, but it really
     * depends on the tokenizer and filters that are used.
     *
     * @return The type of the token.
     */
    public String getType() {
      return type;
    }

    /**
     * Returns the start position of this token within the text it was originally extracted from.
     *
     * @return The start position of this token within the text it was originally extracted from.
     */
    public int getStart() {
      return start;
    }

    /**
     * Returns the end position of this token within the text it was originally extracted from.
     *
     * @return The end position of this token within the text it was originally extracted from.
     */
    public int getEnd() {
      return end;
    }

    /**
     * Returns the position of this token within the produced token stream.
     *
     * @return The position of this token within the produced token stream.
     */
    public int getPosition() {
      return position;
    }

    /**
     * Returns whether this token matches one of the query tokens (if query analysis is performed).
     *
     * @return Whether this token matches one of the query tokens (if query analysis is performed).
     */
    public boolean isMatch() {
      return match;
    }
  }

}
