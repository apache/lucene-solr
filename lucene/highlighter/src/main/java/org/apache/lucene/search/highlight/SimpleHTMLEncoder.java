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
package org.apache.lucene.search.highlight;

/**
 * Simple {@link Encoder} implementation to escape text for HTML output
 *
 */
public class SimpleHTMLEncoder implements Encoder {
  public SimpleHTMLEncoder() {
  }

  @Override
  public String encodeText(String originalText)
  {
    return htmlEncode(originalText);
  }

  /**
   * Encode string into HTML
   */
  public final static String htmlEncode(String plainText)
  {
    if (plainText == null || plainText.length() == 0)
    {
      return "";
    }

    StringBuilder result = new StringBuilder(plainText.length());

    for (int index=0; index<plainText.length(); index++)
    {
      char ch = plainText.charAt(index);

      switch (ch) {
      case '"':
        result.append("&quot;");
        break;
      case '&':
        result.append("&amp;");
        break;
      case '<':
        result.append("&lt;");
        break;
      case '>':
        result.append("&gt;");
        break;
      case '\'':
        result.append("&#x27;");
        break;
      case '/':
        result.append("&#x2F;");
        break;
      default:
        result.append(ch);
      }
    }

    return result.toString();
  }
}