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

package org.apache.solr.search;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class SignificantTermsQParserPluginTest extends SolrTestCaseJ4 {

    @BeforeClass
    public static void setUpCore() throws Exception {
        String tmpSolrHome = createTempDir().toFile().getAbsolutePath();
        FileUtils.copyDirectory(new File(TEST_HOME()), new File(tmpSolrHome).getAbsoluteFile());
        initCore("solrconfig.xml", "schema.xml", new File(tmpSolrHome).getAbsolutePath());
    }

    /**
     * Test the backwards compatibility for a typo in the SignificantTermsQParserPlugin. It will fail if the backwards
     * compatibility is broken.
     */
    @Test
    public void testQParserBackwardsCompatibility() {
        assertEquals("significantTerms", SignificantTermsQParserPlugin.NAME);
        assertEquals(SignificantTermsQParserPlugin.class,
                QParserPlugin.standardPlugins.get(SignificantTermsQParserPlugin.NAME).getClass());
    }

    @Test
    public void testEmptyCollectionDoesNotThrow() throws Exception {
        SolrCore emptyCore = h.getCore();
        QParserPlugin qParserPlugin = QParserPlugin.standardPlugins.get(SignificantTermsQParserPlugin.NAME);
        Map<String, String> params = new HashMap<>();
        params.put("field", "cat");
        QParser parser = qParserPlugin.createParser("", new MapSolrParams(params), new MapSolrParams(new HashMap<>()), null);
        AnalyticsQuery query = (AnalyticsQuery) parser.parse();
        SolrQueryResponse resp = new SolrQueryResponse();

        RefCounted<SolrIndexSearcher> searcher = emptyCore.getSearcher();
        try {
            DelegatingCollector analyticsCollector = query.getAnalyticsCollector(new ResponseBuilder(null, resp, Collections.emptyList()), searcher.get());
            assertNotNull(analyticsCollector);
            analyticsCollector.finish();
            LinkedHashMap<String, Object> expectedValues = new LinkedHashMap<>();
            expectedValues.put("numDocs", 0);
            expectedValues.put("sterms", new ArrayList<String>());
            expectedValues.put("scores", new ArrayList<Integer>());
            expectedValues.put("docFreq", new ArrayList<Integer>());
            expectedValues.put("queryDocFreq", new ArrayList<Double>());
            assertEquals(expectedValues, resp.getValues().get("significantTerms"));
        } finally {
            searcher.decref();
        }

    }

    @Test
    public void testCollectionWithDocuments() throws Exception {
        SolrCore dataCore = h.getCore();
        addTestDocs(dataCore);

        QParserPlugin qParserPlugin = QParserPlugin.standardPlugins.get(SignificantTermsQParserPlugin.NAME);
        Map<String, String> params = new HashMap<>();
        params.put("field", "cat");
        QParser parser = qParserPlugin.createParser("", new MapSolrParams(params), new MapSolrParams(new HashMap<>()), null);
        AnalyticsQuery query = (AnalyticsQuery) parser.parse();
        SolrQueryResponse resp = new SolrQueryResponse();

        ResponseBuilder responseBuilder = new ResponseBuilder(null, resp, Collections.emptyList());
        RefCounted<SolrIndexSearcher> searcher = dataCore.getSearcher();
        try {

            DelegatingCollector analyticsCollector = query.getAnalyticsCollector(responseBuilder, searcher.get());
            assertNotNull(analyticsCollector);
            analyticsCollector.finish();

            LinkedHashMap<String, Object> expectedValues = new LinkedHashMap<>();
            expectedValues.put("numDocs", 1);
            expectedValues.put("sterms", new ArrayList<String>());
            expectedValues.put("scores", new ArrayList<Integer>());
            expectedValues.put("docFreq", new ArrayList<Integer>());
            expectedValues.put("queryDocFreq", new ArrayList<Double>());

            assertEquals(expectedValues, resp.getValues().get("significantTerms"));

        } finally {
            searcher.decref();
        }

        deleteTestDocs(dataCore);
    }

    private void addTestDocs(SolrCore core) throws IOException {
        SolrQueryRequest coreReq = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
        AddUpdateCommand cmd = new AddUpdateCommand(coreReq);
        cmd.solrDoc = new SolrInputDocument();
        cmd.solrDoc.addField("id", "1");
        cmd.solrDoc.addField("cat", "foo");
        core.getUpdateHandler().addDoc(cmd);

        core.getUpdateHandler().commit(new CommitUpdateCommand(coreReq, true));
        coreReq.close();
    }

    private void deleteTestDocs(SolrCore core) throws IOException {
        SolrQueryRequest coreReq = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
        DeleteUpdateCommand cmd = new DeleteUpdateCommand(coreReq);
        cmd.id = "1";
        core.getUpdateHandler().delete(cmd);
        core.getUpdateHandler().commit(new CommitUpdateCommand(coreReq, true));
        coreReq.close();
    }

}
