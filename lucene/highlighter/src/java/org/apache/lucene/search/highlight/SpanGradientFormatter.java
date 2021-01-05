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
 * Formats text with different color intensity depending on the score of the term using the span
 * tag. GradientFormatter uses a bgcolor argument to the font tag which doesn't work in Mozilla,
 * thus this class.
 *
 * @see GradientFormatter
 */
public class SpanGradientFormatter extends GradientFormatter {
  public SpanGradientFormatter(
      float maxScore,
      String minForegroundColor,
      String maxForegroundColor,
      String minBackgroundColor,
      String maxBackgroundColor) {
    super(maxScore, minForegroundColor, maxForegroundColor, minBackgroundColor, maxBackgroundColor);
  }

  @Override
  public String highlightTerm(String originalText, TokenGroup tokenGroup) {
    if (tokenGroup.getTotalScore() == 0) return originalText;
    float score = tokenGroup.getTotalScore();
    if (score == 0) {
      return originalText;
    }

    // try to size sb correctly
    StringBuilder sb = new StringBuilder(originalText.length() + EXTRA);

    sb.append("<span style=\"");
    if (highlightForeground) {
      sb.append("color: ");
      sb.append(getForegroundColorString(score));
      sb.append("; ");
    }
    if (highlightBackground) {
      sb.append("background: ");
      sb.append(getBackgroundColorString(score));
      sb.append("; ");
    }
    sb.append("\">");
    sb.append(originalText);
    sb.append("</span>");
    return sb.toString();
  }

  // guess how much extra text we'll add to the text we're highlighting to try to avoid a
  // StringBuilder resize
  private static final String TEMPLATE =
      "<span style=\"background: #EEEEEE; color: #000000;\">...</span>";
  private static final int EXTRA = TEMPLATE.length();
}
