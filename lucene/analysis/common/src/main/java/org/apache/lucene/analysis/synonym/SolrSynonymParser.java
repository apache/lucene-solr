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
package org.apache.lucene.analysis.synonym;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;

/**
 * Parser for the Solr synonyms format.
 * <ol>
 *   <li> Blank lines and lines starting with '#' are comments.
 *   <li> Explicit mappings match any token sequence on the LHS of "=&gt;"
 *        and replace with all alternatives on the RHS.  These types of mappings
 *        ignore the expand parameter in the constructor.
 *        Example:
 *        <blockquote>i-pod, i pod =&gt; ipod</blockquote>
 *   <li> Equivalent synonyms may be separated with commas and give
 *        no explicit mapping.  In this case the mapping behavior will
 *        be taken from the expand parameter in the constructor.  This allows
 *        the same synonym file to be used in different synonym handling strategies.
 *        Example:
 *        <blockquote>ipod, i-pod, i pod</blockquote>
 * 
 *   <li> Multiple synonym mapping entries are merged.
 *        Example:
 *        <blockquote>
 *         foo =&gt; foo bar<br>
 *         foo =&gt; baz<br><br>
 *         is equivalent to<br><br>
 *         foo =&gt; foo bar, baz
 *        </blockquote>
 *  </ol>
 * @lucene.experimental
 */
public class SolrSynonymParser extends SynonymMap.Parser {
  private final boolean expand;
  
  public SolrSynonymParser(boolean dedup, boolean expand, Analyzer analyzer) {
    super(dedup, analyzer);
    this.expand = expand;
  }

  @Override
  public void parse(Reader in) throws IOException, ParseException {
    LineNumberReader br = new LineNumberReader(in);
    try {
      addInternal(br);
    } catch (IllegalArgumentException e) {
      ParseException ex = new ParseException("Invalid synonym rule at line " + br.getLineNumber(), 0);
      ex.initCause(e);
      throw ex;
    } finally {
      br.close();
    }
  }
  
  private void addInternal(BufferedReader in) throws IOException {
    String line = null;
    while ((line = in.readLine()) != null) {
      if (line.length() == 0 || line.charAt(0) == '#') {
        continue; // ignore empty lines and comments
      }
      
      // TODO: we could process this more efficiently.
      String sides[] = split(line, "=>");
      if (sides.length > 1) { // explicit mapping
        if (sides.length != 2) {
          throw new IllegalArgumentException("more than one explicit mapping specified on the same line");
        }
        String inputStrings[] = split(sides[0], ",");
        CharsRef[] inputs = new CharsRef[inputStrings.length];
        for (int i = 0; i < inputs.length; i++) {
          inputs[i] = analyze(unescape(inputStrings[i]).trim(), new CharsRefBuilder());
        }
        
        String outputStrings[] = split(sides[1], ",");
        CharsRef[] outputs = new CharsRef[outputStrings.length];
        for (int i = 0; i < outputs.length; i++) {
          outputs[i] = analyze(unescape(outputStrings[i]).trim(), new CharsRefBuilder());
        }
        // these mappings are explicit and never preserve original
        for (int i = 0; i < inputs.length; i++) {
          for (int j = 0; j < outputs.length; j++) {
            add(inputs[i], outputs[j], false);
          }
        }
      } else {
        String inputStrings[] = split(line, ",");
        CharsRef[] inputs = new CharsRef[inputStrings.length];
        for (int i = 0; i < inputs.length; i++) {
          inputs[i] = analyze(unescape(inputStrings[i]).trim(), new CharsRefBuilder());
        }
        if (expand) {
          // all pairs
          for (int i = 0; i < inputs.length; i++) {
            for (int j = 0; j < inputs.length; j++) {
              if (i != j) {
                add(inputs[i], inputs[j], true);
              }
            }
          }
        } else {
          // all subsequent inputs map to first one; we also add inputs[0] here
          // so that we "effectively" (because we remove the original input and
          // add back a synonym with the same text) change that token's type to
          // SYNONYM (matching legacy behavior):
          for (int i = 0; i < inputs.length; i++) {
            add(inputs[i], inputs[0], false);
          }
        }
      }
    }
  }
  
  private static String[] split(String s, String separator) {
    ArrayList<String> list = new ArrayList<>(2);
    StringBuilder sb = new StringBuilder();
    int pos=0, end=s.length();
    while (pos < end) {
      if (s.startsWith(separator,pos)) {
        if (sb.length() > 0) {
          list.add(sb.toString());
          sb=new StringBuilder();
        }
        pos+=separator.length();
        continue;
      }

      char ch = s.charAt(pos++);
      if (ch=='\\') {
        sb.append(ch);
        if (pos>=end) break;  // ERROR, or let it go?
        ch = s.charAt(pos++);
      }

      sb.append(ch);
    }

    if (sb.length() > 0) {
      list.add(sb.toString());
    }

    return list.toArray(new String[list.size()]);
  }
  
  private String unescape(String s) {
    if (s.indexOf("\\") >= 0) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < s.length(); i++) {
        char ch = s.charAt(i);
        if (ch == '\\' && i < s.length() - 1) {
          sb.append(s.charAt(++i));
        } else {
          sb.append(ch);
        }
      }
      return sb.toString();
    }
    return s;
  }
}
