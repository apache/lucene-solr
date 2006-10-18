/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.solr.request.QueryResponseWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.TestHarness;

/** Tests the ability to configure multiple query output writers, and select those
 * at query time.
 *
 * @author <a href='mailto:mbaranczak@epublishing.com'> Mike Baranczak </a>
 */
public class OutputWriterTest extends AbstractSolrTestCase {
    
    /** The XML string that's output for testing purposes. */
    public static final String USELESS_OUTPUT = "useless output";
    
    public String getSchemaFile() { return "solr/crazy-path-to-schema.xml"; }
    
    public String getSolrConfigFile() { return "solr/crazy-path-to-config.xml"; }
    
    
    public void testOriginalSolrWriter() {
        lrf.args.put("wt", "standard");
        assertQ(req("foo"), "//response/responseHeader/status");
        
        lrf.args.remove("wt");
        assertQ(req("foo"), "//response/responseHeader/status");
    }
    
    public void testUselessWriter() throws Exception {
        lrf.args.put("wt", "useless");
        String out = h.query(req("foo"));
        assertEquals(USELESS_OUTPUT, out);
    }
    
    public void testTrivialXsltWriter() throws Exception {
        lrf.args.put("wt", "xslt");
        lrf.args.put("tr", "dummy.xsl");
        String out = h.query(req("foo"));
        System.out.println(out);
        assertTrue(out.contains("DUMMY"));
    }
    
    
    ////////////////////////////////////////////////////////////////////////////
    /** An output writer that doesn't do anything useful. */
    
    public static class UselessOutputWriter implements QueryResponseWriter {
        
        public UselessOutputWriter() {}

        public void init(NamedList n) {}
        
        public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response)
        throws IOException {
            writer.write(USELESS_OUTPUT);
        }

      public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
        return CONTENT_TYPE_TEXT_UTF8;
      }

    }
    
}
