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
import java.util.Iterator;
import java.util.List;

/**
 * A test case for the {@link FieldAnalysisResponse} class.
 *
 *
 * @since solr 1.4
 */
@SuppressWarnings("unchecked")
public class FieldAnalysisResponseTest extends SolrTestCase {

  /**
   * Tests the {@link FieldAnalysisResponse#setResponse(org.apache.solr.common.util.NamedList)} method.
   */
  @Test
  public void testSetResponse() throws Exception {

    // the parsing of the analysis phases is already tested in the AnalysisResponseBaseTest. So we can just fake
    // the phases list here and use it.
    final List<AnalysisResponseBase.AnalysisPhase> phases = new ArrayList<>(1);
    AnalysisResponseBase.AnalysisPhase expectedPhase = new AnalysisResponseBase.AnalysisPhase("Tokenizer");
    phases.add(expectedPhase);

    @SuppressWarnings({"rawtypes"})
    NamedList responseNL = buildResponse();
    FieldAnalysisResponse response = new FieldAnalysisResponse() {
      @Override
      protected List<AnalysisPhase> buildPhases(NamedList<Object> phaseNL) {
        return phases;
      }
    };

    response.setResponse(responseNL);

    assertEquals(1, response.getFieldNameAnalysisCount());
    FieldAnalysisResponse.Analysis analysis = response.getFieldNameAnalysis("name");
    Iterator<AnalysisResponseBase.AnalysisPhase> iter = analysis.getIndexPhases().iterator();
    assertTrue(iter.hasNext());
    assertSame(expectedPhase, iter.next());
    assertFalse(iter.hasNext());
    iter = analysis.getQueryPhases().iterator();
    assertTrue(iter.hasNext());
    assertSame(expectedPhase, iter.next());
    assertFalse(iter.hasNext());

    analysis = response.getFieldTypeAnalysis("text");
    iter = analysis.getIndexPhases().iterator();
    assertTrue(iter.hasNext());
    assertSame(expectedPhase, iter.next());
    assertFalse(iter.hasNext());
    iter = analysis.getQueryPhases().iterator();
    assertTrue(iter.hasNext());
    assertSame(expectedPhase, iter.next());
    assertFalse(iter.hasNext());
  }

  //================================================ Helper Methods ==================================================

  @SuppressWarnings({"rawtypes"})
  private NamedList buildResponse() {
    NamedList response = new NamedList();

    NamedList responseHeader = new NamedList();
    response.add("responseHeader", responseHeader);

    NamedList params = new NamedList();
    responseHeader.add("params", params);
    params.add("analysis.showmatch", "true");
    params.add("analysis.query", "the query");
    params.add("analysis.fieldname", "name");
    params.add("analysis.fieldvalue", "The field value");
    params.add("analysis.fieldtype", "text");

    responseHeader.add("status", 0);
    responseHeader.add("QTime", 66);

    NamedList analysis = new NamedList();
    response.add("analysis", analysis);

    NamedList fieldTypes = new NamedList();
    analysis.add("field_types", fieldTypes);
    NamedList text = new NamedList();
    fieldTypes.add("text", text);
    NamedList index = new NamedList();
    text.add("index", index);
    NamedList query = new NamedList();
    text.add("query", query);

    NamedList fieldNames = new NamedList();
    analysis.add("field_names", fieldNames);
    NamedList name = new NamedList();
    fieldNames.add("name", name);
    index = new NamedList();
    name.add("index", index);
    query = new NamedList();
    name.add("query", query);

    return response;
  }
}
