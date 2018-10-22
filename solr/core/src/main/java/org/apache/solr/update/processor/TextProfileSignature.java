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
package org.apache.solr.update.processor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.solr.common.params.SolrParams;

/**
 * <p>This implementation is copied from Apache Nutch. </p>
 * <p>An implementation of a page signature. It calculates an MD5 hash
 * of a plain text "profile" of a page.</p>
 * <p>The algorithm to calculate a page "profile" takes the plain text version of
 * a page and performs the following steps:
 * <ul>
 * <li>remove all characters except letters and digits, and bring all characters
 * to lower case,</li>
 * <li>split the text into tokens (all consecutive non-whitespace characters),</li>
 * <li>discard tokens equal or shorter than MIN_TOKEN_LEN (default 2 characters),</li>
 * <li>sort the list of tokens by decreasing frequency,</li>
 * <li>round down the counts of tokens to the nearest multiple of QUANT
 * (<code>QUANT = QUANT_RATE * maxFreq</code>, where <code>QUANT_RATE</code> is 0.01f
 * by default, and <code>maxFreq</code> is the maximum token frequency). If
 * <code>maxFreq</code> is higher than 1, then QUANT is always higher than 2 (which
 * means that tokens with frequency 1 are always discarded).</li>
 * <li>tokens, which frequency after quantization falls below QUANT, are discarded.</li>
 * <li>create a list of tokens and their quantized frequency, separated by spaces,
 * in the order of decreasing frequency.</li>
 * </ul>
 * This list is then submitted to an MD5 hash calculation.*/
public class TextProfileSignature extends MD5Signature {

  private float quantRate;
  private float minTokenLen;

  @Override
  public void init(SolrParams params) {
    quantRate = params.getFloat("quantRate", 0.01f);
    minTokenLen = params.getInt("minTokenLen", 2);
  }

  
  @Override
  public byte[] getSignature() {
    return super.getSignature();
  }

  @Override
  public void add(String content) {
    HashMap<String, Token> tokens = new HashMap<>();

    StringBuilder curToken = new StringBuilder();
    int maxFreq = 0;
    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);
      if (Character.isLetterOrDigit(c)) {
        curToken.append(Character.toLowerCase(c));
      } else {
        if (curToken.length() > 0) {
          if (curToken.length() > minTokenLen) {
            // add it
            String s = curToken.toString();
            Token tok = tokens.get(s);
            if (tok == null) {
              tok = new Token(0, s);
              tokens.put(s, tok);
            }
            tok.cnt++;
            if (tok.cnt > maxFreq)
              maxFreq = tok.cnt;
          }
          curToken.setLength(0);
        }
      }
    }
    // check the last token
    if (curToken.length() > minTokenLen) {
      // add it
      String s = curToken.toString();
      Token tok = tokens.get(s);
      if (tok == null) {
        tok = new Token(0, s);
        tokens.put(s, tok);
      }
      tok.cnt++;
      if (tok.cnt > maxFreq)
        maxFreq = tok.cnt;
    }
    Iterator<Token> it = tokens.values().iterator();
    ArrayList<Token> profile = new ArrayList<>();
    // calculate the QUANT value
    int quant = Math.round(maxFreq * quantRate);
    if (quant < 2) {
      if (maxFreq > 1)
        quant = 2;
      else
        quant = 1;
    }
    while (it.hasNext()) {
      Token t = it.next();
      // round down to the nearest QUANT
      t.cnt = (t.cnt / quant) * quant;
      // discard the frequencies below the QUANT
      if (t.cnt < quant) {
        continue;
      }
      profile.add(t);
    }
    Collections.sort(profile, new TokenComparator());
    StringBuilder newText = new StringBuilder();
    it = profile.iterator();
    while (it.hasNext()) {
      Token t = it.next();
      if (newText.length() > 0)
        newText.append("\n");
      newText.append(t.toString());
    }

    super.add(newText.toString());
  }

  private static class Token {
    public int cnt;
    public String val;

    public Token(int cnt, String val) {
      this.cnt = cnt;
      this.val = val;
    }

    @Override
    public String toString() {
      return val + " " + cnt;
    }
  }

  private static class TokenComparator implements Comparator<Token> {
    @Override
    public int compare(Token t1, Token t2) {
      return t2.cnt - t1.cnt;
    }
  }

}
