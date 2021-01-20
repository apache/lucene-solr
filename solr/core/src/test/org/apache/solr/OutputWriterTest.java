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
package org.apache.solr;

import java.io.IOException;
import java.io.Writer;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginBag;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests the ability to configure multiple query output writers, and select those
 * at query time.
 *
 */
public class OutputWriterTest extends SolrTestCaseJ4 {
    
    /** The XML string that's output for testing purposes. */
    public static final String USELESS_OUTPUT = "useless output";
    
    @BeforeClass
    public static void beforeClass() throws Exception {
      initCore("solr/crazy-path-to-config.xml","solr/crazy-path-to-schema.xml");
    }
    
    
    /** 
     * responseHeader has changed in SOLR-59, check old and new variants,
     * In SOLR-2413, we removed support for the deprecated versions
     */
    @Test
    public void testSOLR59responseHeaderVersions() {
        // default version is 2.2, with "new" responseHeader
        lrf.args.remove(CommonParams.VERSION);
        lrf.args.put("wt", "standard");
        assertQ(req("foo"), "/response/lst[@name='responseHeader']/int[@name='status'][.='0']");
        lrf.args.remove("wt");
        assertQ(req("foo"), "/response/lst[@name='responseHeader']/int[@name='QTime']");
        
        // and explicit 2.2 works as default  
        //lrf.args.put("version", "2.2");
        lrf.args.put("wt", "standard");
        assertQ(req("foo"), "/response/lst[@name='responseHeader']/int[@name='status'][.='0']");
        lrf.args.remove("wt");
        assertQ(req("foo"), "/response/lst[@name='responseHeader']/int[@name='QTime']");
    }
    
    @Test
    public void testUselessWriter() throws Exception {
        lrf.args.put("wt", "useless");
        String out = h.query(req("foo"));
        assertEquals(USELESS_OUTPUT, out);
    }
    
    @Test
    public void testTrivialXsltWriter() throws Exception {
        lrf.args.put("wt", "xslt");
        lrf.args.put("tr", "dummy.xsl");
        String out = h.query(req("foo"));
        // System.out.println(out);
        assertTrue(out.contains("DUMMY"));
    }
    
    @Test
    public void testTrivialXsltWriterInclude() throws Exception {
        lrf.args.put("wt", "xslt");
        lrf.args.put("tr", "dummy-using-include.xsl");
        String out = h.query(req("foo"));
        // System.out.println(out);
        assertTrue(out.contains("DUMMY"));
    }

    public void testLazy() {
        PluginBag.PluginHolder<QueryResponseWriter> qrw = h.getCore().getResponseWriters().getRegistry().get("useless");
        assertTrue("Should be a lazy class", qrw instanceof PluginBag.LazyPluginHolder);

        qrw = h.getCore().getResponseWriters().getRegistry().get("xml");
        assertTrue("Should not be a lazy class", qrw.isLoaded());
        assertTrue("Should not be a lazy class", qrw.getClass() == PluginBag.PluginHolder.class);

    }
    
    ////////////////////////////////////////////////////////////////////////////
    /** An output writer that doesn't do anything useful. */
    
    public static class UselessOutputWriter implements QueryResponseWriter {
        
        public UselessOutputWriter() {}

        @Override
        public void init(@SuppressWarnings({"rawtypes"})NamedList n) {}
        
        @Override
        public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response)
        throws IOException {
            writer.write(USELESS_OUTPUT);
        }

      @Override
      public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
        return CONTENT_TYPE_TEXT_UTF8;
      }

    }
    
}
