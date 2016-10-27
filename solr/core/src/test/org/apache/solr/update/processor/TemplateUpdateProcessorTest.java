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

package org.apache.solr.update.processor;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;

public class TemplateUpdateProcessorTest extends SolrTestCaseJ4 {
  public void testSimple() throws Exception {

    AddUpdateCommand cmd = new AddUpdateCommand(new LocalSolrQueryRequest(null,
        new ModifiableSolrParams()
            .add("processor", "Template")
            .add("Template.field", "id:${firstName}_${lastName}")
            .add("Template.field", "another:${lastName}_${firstName}")
            .add("Template.field", "missing:${lastName}_${unKnown}")

    ));
    cmd.solrDoc = new SolrInputDocument();
    cmd.solrDoc.addField("firstName", "Tom");
    cmd.solrDoc.addField("lastName", "Cruise");

    new TemplateUpdateProcessorFactory().getInstance(cmd.getReq(), new SolrQueryResponse(), null).processAdd(cmd);
    assertEquals("Tom_Cruise", cmd.solrDoc.getFieldValue("id"));
    assertEquals("Cruise_Tom", cmd.solrDoc.getFieldValue("another"));
    assertEquals("Cruise_", cmd.solrDoc.getFieldValue("missing"));

  }
}
