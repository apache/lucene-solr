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
package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

public class IndexSchemaRuntimeFieldTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  @Test
  public void testRuntimeFieldCreation() {
    // any field manipulation needs to happen when you know the core will not
    // be accepting any requests.  Typically this is done within the inform()
    // method.  Since this is a single threaded test, we can change the fields
    // willi-nilly

    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    final String fieldName = "runtimefield";
    SchemaField sf = new SchemaField( fieldName, schema.getFieldTypes().get( "string" ) );
    schema.getFields().put( fieldName, sf );

    // also register a new copy field (from our new field)
    schema.registerCopyField( fieldName, "dynamic_runtime" );
    schema.refreshAnalyzers();

    assertU(adoc("id", "10", "title", "test", fieldName, "aaa"));
    assertU(commit());

    SolrQuery query = new SolrQuery( fieldName+":aaa" );
    query.set( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, query );

    assertQ("Make sure they got in", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='10']"
            );

    // Check to see if our copy field made it out safely
    query.setQuery( "dynamic_runtime:aaa" );
    assertQ("Make sure they got in", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='10']"
            );
    clearIndex();
  }
}
