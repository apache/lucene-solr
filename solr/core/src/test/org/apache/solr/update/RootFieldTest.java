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

package org.apache.solr.update;

import org.apache.solr.EmbeddedSolrServerTestBase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.is;

public class RootFieldTest extends EmbeddedSolrServerTestBase {
  private static boolean useRootSchema;
  private static final String MESSAGE = "Update handler should create and process _root_ field " +
      "unless there is no such a field in schema";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static boolean expectRoot() {
    return useRootSchema;
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    useRootSchema = random().nextBoolean();
    // schema15.xml declares _root_ field, while schema-rest.xml does not.
    String schema = useRootSchema ? "schema15.xml" : "schema-rest.xml";
    initCore("solrconfig.xml", schema);
  }

  @Test
  public void testLegacyBlockProcessing() throws Exception
  {
    SolrClient client = getSolrClient();
    client.deleteByQuery("*:*");// delete everything!

    // Add child free doc
    SolrInputDocument docToUpdate = new SolrInputDocument();
    String docId = "11";
    docToUpdate.addField( "id", docId);
    docToUpdate.addField( "name", "child free doc" );
    client.add(docToUpdate);
    client.commit();

    SolrQuery query = new SolrQuery();
    query.setQuery( "*:*" );
    query.set( CommonParams.FL, "id,name,_root_" );

    SolrDocumentList results = client.query(query).getResults();
    assertThat(results.getNumFound(), is(1L));
    SolrDocument foundDoc = results.get( 0 );

    // Check retrieved field values
    assertThat(foundDoc.getFieldValue( "id" ), is(docId));
    assertThat(foundDoc.getFieldValue( "name" ), is("child free doc"));

    String expectedRootValue = expectRoot() ? docId : null;
    assertThat(MESSAGE, foundDoc.getFieldValue( "_root_" ), is(expectedRootValue));

    // Update the doc
    docToUpdate.setField( "name", "updated doc" );
    client.add(docToUpdate);
    client.commit();

    results = client.query(query).getResults();
    assertEquals( 1, results.getNumFound() );
    foundDoc = results.get( 0 );

    // Check updated field values
    assertThat(foundDoc.getFieldValue( "id" ), is(docId));
    assertThat(foundDoc.getFieldValue( "name" ), is("updated doc"));
    assertThat(MESSAGE, foundDoc.getFieldValue( "_root_" ), is(expectedRootValue));
  }

  @Test
  public void testUpdateWithChildDocs() throws Exception {
    SolrClient client = getSolrClient();
    client.deleteByQuery("*:*");// delete everything!

    // Add child free doc
    SolrInputDocument docToUpdate = new SolrInputDocument();
    String docId = "11";
    docToUpdate.addField( "id", docId);
    docToUpdate.addField( "name", "parent doc with a child" );
    SolrInputDocument child = new SolrInputDocument();
    child.addField("id", "111");
    child.addField("name", "child doc");
    docToUpdate.addChildDocument(child);
    if (!useRootSchema) {
      thrown.expect(SolrException.class);
      thrown.expectMessage("Unable to index docs with children:" +
          " the schema must include definitions for both a uniqueKey field" +
          " and the '_root_' field, using the exact same fieldType");
    }
    client.add(docToUpdate);
    client.commit();
  }

}
