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
package org.apache.lucene.analysis.ko.util;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing CSV text
 */
public final class CSVUtil {
  private static final char QUOTE = '"';
  
  private static final char COMMA = ',';
  
  private static final Pattern QUOTE_REPLACE_PATTERN = Pattern.compile("^\"([^\"]+)\"$");
  
  private static final String ESCAPED_QUOTE = "\"\"";
  
  private CSVUtil() {} // no instance!!!
  
  /**
   * Parse CSV line
   * @param line line containing csv-encoded data
   * @return Array of values
   */
  public static String[] parse(String line) {
    boolean insideQuote = false;
    ArrayList<String> result = new ArrayList<>();
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
        sb.setLength(0);
        continue;
      }
      
      sb.append(c);
    }
    
    result.add(sb.toString());
    
    // Validate
    if(quoteCount % 2 != 0) {
      return new String[0];
    }
    
    return result.toArray(new String[0]);
  }
  
  private static String unQuoteUnEscape(String original) {
    String result = original;
    
    // Unquote
    if (result.indexOf('\"') >= 0) {
      Matcher m = QUOTE_REPLACE_PATTERN.matcher(original);
      if(m.matches()) {
        result = m.group(1);
      }
    
      // Unescape
      if (result.contains(ESCAPED_QUOTE)) {
        result = result.replace(ESCAPED_QUOTE, "\"");
      }
    }
    
    return result;
    
  }
}
