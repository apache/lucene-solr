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
package org.apache.solr.handler;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.commons.lang3.ArrayUtils;

/**
 * A base class for all analysis request handler tests.
 *
 *
 * @since solr 1.4
 */
public abstract class AnalysisRequestHandlerTestBase extends SolrTestCaseJ4 {

  protected void assertToken(@SuppressWarnings({"rawtypes"})NamedList token, TokenInfo info) {
    assertEquals(info.getText(), token.get("text"));
    if (info.getRawText() != null) {
      assertEquals(info.getRawText(), token.get("raw_text"));
    }
    assertEquals(info.getType(), token.get("type"));
    assertEquals(info.getStart(), token.get("start"));
    assertEquals(info.getEnd(), token.get("end"));
    assertEquals(info.getPosition(), token.get("position"));
    assertArrayEquals(info.getPositionHistory(), ArrayUtils.toPrimitive((Integer[]) token.get("positionHistory")));
    if (info.isMatch()) {
      assertEquals(Boolean.TRUE, token.get("match"));
    }
    if (info.getPayload() != null) {
      assertEquals(info.getPayload(), token.get("payload"));
    }
  }


  //================================================= Inner Classes ==================================================

  protected static class TokenInfo {

    private String text;
    private String rawText;
    private String type;
    private int start;
    private int end;
    private String payload;
    private int position;
    private int[] positionHistory;
    private boolean match;

    public TokenInfo(
            String text,
            String rawText,
            String type,
            int start,
            int end,
            int position,
            int[] positionHistory,
            String payload,
            boolean match) {

      this.text = text;
      this.rawText = rawText;
      this.type = type;
      this.start = start;
      this.end = end;
      this.position = position;
      this.positionHistory = positionHistory;
      this.payload = payload;
      this.match = match;
    }

    public String getText() {
      return text;
    }

    public String getRawText() {
      return rawText;
    }

    public String getType() {
      return type;
    }

    public int getStart() {
      return start;
    }

    public int getEnd() {
      return end;
    }

    public String getPayload() {
      return payload;
    }

    public int getPosition() {
      return position;
    }

    public int[] getPositionHistory() {
      return positionHistory;
    }

    public boolean isMatch() {
      return match;
    }
  }

}
