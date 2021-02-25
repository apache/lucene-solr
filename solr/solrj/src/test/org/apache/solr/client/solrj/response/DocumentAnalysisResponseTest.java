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
 * A test for the {@link DocumentAnalysisResponse} class.
 *
 *
 * @since solr 1.4
 */
public class DocumentAnalysisResponseTest extends SolrTestCase {

  /**
   * Tests the {@link DocumentAnalysisResponse#setResponse(org.apache.solr.common.util.NamedList)} method
   */
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testSetResponse() throws Exception {

    // the parsing of the analysis phases is already tested in the AnalysisResponseBaseTest. So we can just fake
    // the phases list here and use it.
    final List<AnalysisResponseBase.AnalysisPhase> phases = new ArrayList<>(1);
    AnalysisResponseBase.AnalysisPhase expectedPhase = new AnalysisResponseBase.AnalysisPhase("Tokenizer");
    phases.add(expectedPhase);

    NamedList responseNL = buildResponse();
    DocumentAnalysisResponse response = new DocumentAnalysisResponse() {

      @Override
      protected List<AnalysisPhase> buildPhases(NamedList<Object> phaseNL) {
        return phases;
      }
    };

    response.setResponse(responseNL);

    assertEquals(1, response.getDocumentAnalysesCount());

    DocumentAnalysisResponse.DocumentAnalysis documentAnalysis = response.getDocumentAnalysis("1");
    assertEquals("1", documentAnalysis.getDocumentKey());
    assertEquals(3, documentAnalysis.getFieldAnalysesCount());

    DocumentAnalysisResponse.FieldAnalysis fieldAnalysis = documentAnalysis.getFieldAnalysis("id");
    assertEquals("id", fieldAnalysis.getFieldName());
    assertEquals(1, fieldAnalysis.getQueryPhasesCount());
    AnalysisResponseBase.AnalysisPhase phase = fieldAnalysis.getQueryPhases().iterator().next();
    assertSame(expectedPhase, phase);
    assertEquals(1, fieldAnalysis.getValueCount());
    assertEquals(1, fieldAnalysis.getIndexPhasesCount("1"));
    phase = fieldAnalysis.getIndexPhases("1").iterator().next();
    assertSame(expectedPhase, phase);

    fieldAnalysis = documentAnalysis.getFieldAnalysis("name");
    assertEquals("name", fieldAnalysis.getFieldName());
    assertEquals(1, fieldAnalysis.getQueryPhasesCount());
    phase = fieldAnalysis.getQueryPhases().iterator().next();
    assertSame(expectedPhase, phase);
    assertEquals(2, fieldAnalysis.getValueCount());
    assertEquals(1, fieldAnalysis.getIndexPhasesCount("name value 1"));
    phase = fieldAnalysis.getIndexPhases("name value 1").iterator().next();
    assertSame(expectedPhase, phase);
    assertEquals(1, fieldAnalysis.getIndexPhasesCount("name value 2"));
    phase = fieldAnalysis.getIndexPhases("name value 2").iterator().next();
    assertSame(expectedPhase, phase);

    fieldAnalysis = documentAnalysis.getFieldAnalysis("text");
    assertEquals("text", fieldAnalysis.getFieldName());
    assertEquals(1, fieldAnalysis.getQueryPhasesCount());
    phase = fieldAnalysis.getQueryPhases().iterator().next();
    assertSame(expectedPhase, phase);
    assertEquals(1, fieldAnalysis.getValueCount());
    assertEquals(1, fieldAnalysis.getIndexPhasesCount("text value"));
    phase = fieldAnalysis.getIndexPhases("text value").iterator().next();
    assertSame(expectedPhase, phase);
  }

  //================================================ Helper Methods ==================================================

  @SuppressWarnings({"unchecked", "rawtypes"})
  private NamedList buildResponse() {

    NamedList response = new NamedList();

    NamedList responseHeader = new NamedList();
    response.add("responseHeader", responseHeader);

    NamedList params = new NamedList();
    responseHeader.add("params", params);
    params.add("analysis.showmatch", "true");
    params.add("analysis.query", "the query");

    responseHeader.add("status", 0);
    responseHeader.add("QTime", 105);

    NamedList analysis = new NamedList();
    response.add("analysis", analysis);

    NamedList doc1 = new NamedList();

    analysis.add("1", doc1);
    NamedList id = new NamedList();
    doc1.add("id", id);
    NamedList query = new NamedList();
    id.add("query", query);
    NamedList index = new NamedList();
    id.add("index", index);
    NamedList idValue = new NamedList();
    index.add("1", idValue);

    NamedList name = new NamedList();
    doc1.add("name", name);
    query = new NamedList();
    name.add("query", query);
    index = new NamedList();
    name.add("index", index);
    NamedList nameValue1 = new NamedList();
    index.add("name value 1", nameValue1);
    NamedList nameValue2 = new NamedList();
    index.add("name value 2", nameValue2);

    NamedList text = new NamedList();
    doc1.add("text", text);
    query = new NamedList();
    text.add("query", query);
    index = new NamedList();
    text.add("index", index);
    NamedList textValue = new NamedList();
    index.add("text value", textValue);

    return response;
  }
}
