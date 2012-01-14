package org.apache.lucene.analysis.kuromoji.util;

/**
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

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CSVUtil {
  private static final char QUOTE = '"';
  
  private static final char COMMA = ',';
  
  private static final Pattern QUOTE_REPLACE_PATTERN = Pattern.compile("^\"([^\"]+)\"$");
  
  private static final String ESCAPED_QUOTE = "\"\"";
  
  private CSVUtil() {} // no instance!!!
  
  /**
   * Parse CSV line
   * @param line
   * @return Array of values
   */
  public static String[] parse(String line) {
    boolean insideQuote = false;
    ArrayList<String> result = new ArrayList<String>();		
    int quoteCount = 0;
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      
      if(c == QUOTE) {
        insideQuote = !insideQuote;
        quoteCount++;
      }
      
      if(c == COMMA && !insideQuote) {
        String value = sb.toString();
        value = unQuoteUnEscape(value);
        result.add(value);
        sb = new StringBuilder();
        continue;
      }
      
      sb.append(c);
    }
    
    result.add(sb.toString());
    
    // Validate
    if(quoteCount % 2 != 0) {
      return new String[0];
    }
    
    return result.toArray(new String[result.size()]);
  }
  
  private static String unQuoteUnEscape(String original) {
    String result = original;
    
    // Unquote
    Matcher m = QUOTE_REPLACE_PATTERN.matcher(original);
    if(m.matches()) {
      result = m.group(1);
    }
    
    // Unescape
    result = result.replaceAll(ESCAPED_QUOTE, "\"");
    
    return result;
    
  }
  
  /**
   * Quote and escape input value for CSV
   * @param original
   */
  public static String quoteEscape(String original) {
    String result = original.replaceAll("\"", ESCAPED_QUOTE);
    if(result.indexOf(COMMA) >= 0) {
      result = "\"" + result + "\"";
    }
    return result;
  }
  
}
