/**
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

import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.io.IOException;
import java.io.StringWriter;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.HashMap;

public class IndexInfoRequestHandlerTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  public void setUp() throws Exception {
    super.setUp();
    lrf = h.getRequestFactory("indexinfo", 0, 0);
  }

  public void testIndexInfo() throws Exception {

    assertU(adoc("id", "529",
                 "field_t", "what's inside?",
                 "subject", "info"
                 ));
    assertU(commit());

    assertQ("index info",
            req("foo")
            ,"//lst[@name='fields']/lst[@name='field_t']"
            ,"//lst[@name='index']/int[@name='numDocs'][.='1']"
            );
  }

}
