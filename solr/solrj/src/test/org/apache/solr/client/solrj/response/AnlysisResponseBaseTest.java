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

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * A Test case for the {@link AnalysisResponseBase} class.
 *
 *
 * @since solr 1.4
 */
@SuppressWarnings("unchecked")
public class AnlysisResponseBaseTest extends SolrTestCase {

  /**
   * Tests the {@link AnalysisResponseBase#buildTokenInfo(org.apache.solr.common.util.NamedList)} method.
   */
  @Test
  public void testBuildTokenInfo() throws Exception {

    @SuppressWarnings({"rawtypes"})
    NamedList tokenNL = new NamedList();
    tokenNL.add("text", "JUMPING");
    tokenNL.add("type", "word");
    tokenNL.add("start", 0);
    tokenNL.add("end", 7);
    tokenNL.add("position", 1);

    AnalysisResponseBase response = new AnalysisResponseBase();

    AnalysisResponseBase.TokenInfo tokenInfo = response.buildTokenInfo(tokenNL);
    assertEquals("JUMPING", tokenInfo.getText());
    assertEquals(null, tokenInfo.getRawText());
    assertEquals("word", tokenInfo.getType());
    assertEquals(0, tokenInfo.getStart());
    assertEquals(7, tokenInfo.getEnd());
    assertEquals(1, tokenInfo.getPosition());
    assertFalse(tokenInfo.isMatch());

    tokenNL.add("rawText", "JUMPING1");
    tokenNL.add("match", true);

    tokenInfo = response.buildTokenInfo(tokenNL);
    assertEquals("JUMPING", tokenInfo.getText());
    assertEquals("JUMPING1", tokenInfo.getRawText());
    assertEquals("word", tokenInfo.getType());
    assertEquals(0, tokenInfo.getStart());
    assertEquals(7, tokenInfo.getEnd());
    assertEquals(1, tokenInfo.getPosition());
    assertTrue(tokenInfo.isMatch());
  }

  /**
   * Tests the {@link AnalysisResponseBase#buildPhases(org.apache.solr.common.util.NamedList)} )} method.
   */
  @Test
  public void testBuildPhases() throws Exception {

    final AnalysisResponseBase.TokenInfo tokenInfo = new AnalysisResponseBase.TokenInfo("text", null, "type", 0, 3, 1, false);
    @SuppressWarnings({"rawtypes"})
    NamedList nl = new NamedList();
    nl.add("Tokenizer", buildFakeTokenInfoList(6));
    nl.add("Filter1", buildFakeTokenInfoList(5));
    nl.add("Filter2", buildFakeTokenInfoList(4));
    nl.add("Filter3", buildFakeTokenInfoList(3));

    AnalysisResponseBase response = new AnalysisResponseBase() {
      @Override
      protected TokenInfo buildTokenInfo(@SuppressWarnings({"rawtypes"})NamedList tokenNL) {
        return tokenInfo;
      }
    };

    List<AnalysisResponseBase.AnalysisPhase> phases = response.buildPhases(nl);

    assertEquals(4, phases.size());
    assertPhase(phases.get(0), "Tokenizer", 6, tokenInfo);
    assertPhase(phases.get(1), "Filter1", 5, tokenInfo);
    assertPhase(phases.get(2), "Filter2", 4, tokenInfo);
    assertPhase(phases.get(3), "Filter3", 3, tokenInfo);
  }

  /**
   * Tests the {@link AnalysisResponseBase#buildPhases(org.apache.solr.common.util.NamedList)} )}
   * method for the special case of CharacterFilter.
   */
  @Test
  public void testCharFilterBuildPhases() throws Exception {
    @SuppressWarnings({"rawtypes"})
    NamedList nl = new NamedList();
    nl.add("CharFilter1", "CharFilterOutput"); //not list of tokens
    AnalysisResponseBase response = new AnalysisResponseBase();
    List<AnalysisResponseBase.AnalysisPhase> phases = response.buildPhases(nl);
    assertEquals(1, phases.size());
  }

  //================================================ Helper Methods ==================================================

  @SuppressWarnings({"rawtypes"})
  private List<NamedList> buildFakeTokenInfoList(int numberOfTokens) {
    List<NamedList> list = new ArrayList<>(numberOfTokens);
    for (int i = 0; i < numberOfTokens; i++) {
      list.add(new NamedList());
    }
    return list;
  }

  private void assertPhase(AnalysisResponseBase.AnalysisPhase phase, String expectedClassName, int expectedTokenCount, AnalysisResponseBase.TokenInfo expectedToken) {

    assertEquals(expectedClassName, phase.getClassName());
    List<AnalysisResponseBase.TokenInfo> tokens = phase.getTokens();
    assertEquals(expectedTokenCount, tokens.size());
    for (AnalysisResponseBase.TokenInfo token : tokens) {
      assertSame(expectedToken, token);
    }
  }
}
