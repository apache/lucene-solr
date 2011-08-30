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

package org.apache.solr.handler;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.noggit.JSONParser;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.junit.BeforeClass;
import org.junit.Test;

public class JsonLoaderTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }
  
  static String input = ("{\n" +
      "\n" +
      "'add': {\n" +
      "  'doc': {\n" +
      "    'bool': true,\n" +
      "    'f0': 'v0',\n" +
      "    'f2': {\n" +
      "      'boost': 2.3,\n" +
      "      'value': 'test'\n" +
      "    },\n" +
      "    'array': [ 'aaa', 'bbb' ],\n" +
      "    'boosted': {\n" +
      "      'boost': 6.7,\n" +
      "      'value': [ 'aaa', 'bbb' ]\n" +
      "    }\n" +
      "  }\n" +
      "},\n" +
      "'add': {\n" +
      "  'commitWithin': 1234,\n" +
      "  'overwrite': false,\n" +
      "  'boost': 3.45,\n" +
      "  'doc': {\n" +
      "    'f1': 'v1',\n" +
      "    'f1': 'v2',\n" +
      "    'f2': null\n" +
      "  }\n" +
      "},\n" +
      "\n" +
      "'commit': {},\n" +
      "'optimize': { 'waitSearcher':false },\n" +
      "\n" +
      "'delete': { 'id':'ID' },\n" +
      "'delete': { 'query':'QUERY' },\n" +
      "'rollback': {}\n" +
      "\n" +
      "}\n" +
      "").replace('\'', '"');


  public void testParsing() throws Exception
  {
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader( req, p );
    loader.load(req, rsp, new ContentStreamBase.StringStream(input));

    assertEquals( 2, p.addCommands.size() );
    
    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField( "boosted" );
    assertEquals(6.7f, f.getBoost());
    assertEquals(2, f.getValues().size());

    // 
    add = p.addCommands.get(1);
    d = add.solrDoc;
    f = d.getField( "f1" );
    assertEquals(2, f.getValues().size());
    assertEquals(3.45f, d.getDocumentBoost());
    assertEquals(false, add.overwrite);

    assertEquals(0, d.getField("f2").getValueCount());

    // parse the commit commands
    assertEquals( 2, p.commitCommands.size() );
    CommitUpdateCommand commit = p.commitCommands.get( 0 );
    assertFalse( commit.optimize );
    assertTrue( commit.waitSearcher );
    
    commit = p.commitCommands.get( 1 );
    assertTrue( commit.optimize );
    assertFalse( commit.waitSearcher );
    

    // DELETE COMMANDS
    assertEquals( 2, p.deleteCommands.size() );
    DeleteUpdateCommand delete = p.deleteCommands.get( 0 );
    assertEquals( delete.id, "ID" );
    assertEquals( delete.query, null );
    
    delete = p.deleteCommands.get( 1 );
    assertEquals( delete.id, null );
    assertEquals( delete.query, "QUERY" );

    // ROLLBACK COMMANDS
    assertEquals( 1, p.rollbackCommands.size() );

    req.close();
  }


  public void testSimpleFormat() throws Exception
  {
    String str = "[{'id':'1'},{'id':'2'}]".replace('\'', '"');
    SolrQueryRequest req = req("commitWithin","100", "overwrite","false");
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader( req, p );
    loader.load(req, rsp, new ContentStreamBase.StringStream(str));

    assertEquals( 2, p.addCommands.size() );

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField( "id" );
    assertEquals("1", f.getValue());
    assertEquals(add.commitWithin, 100);
    assertEquals(add.overwrite, false);

    add = p.addCommands.get(1);
    d = add.solrDoc;
    f = d.getField( "id" );
    assertEquals("2", f.getValue());
    assertEquals(add.commitWithin, 100);
    assertEquals(add.overwrite, false);

    req.close();
  }

  public void testSimpleFormatInAdd() throws Exception
  {
    String str = "{'add':[{'id':'1'},{'id':'2'}]}".replace('\'', '"');
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader( req, p );
    loader.load(req, rsp, new ContentStreamBase.StringStream(str));

    assertEquals( 2, p.addCommands.size() );

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField( "id" );
    assertEquals("1", f.getValue());
    assertEquals(add.commitWithin, -1);
    assertEquals(add.overwrite, true);

    add = p.addCommands.get(1);
    d = add.solrDoc;
    f = d.getField( "id" );
    assertEquals("2", f.getValue());
    assertEquals(add.commitWithin, -1);
    assertEquals(add.overwrite, true);

    req.close();
  }

  @Test
  public void testNullValues() throws Exception {
    updateJ("[{'id':'10','foo_s':null,'foo2_s':['hi',null,'there']}]".replace('\'', '"'), params("commit","true"));
    assertJQ(req("q","id:10", "fl","foo_s,foo2_s")
        ,"/response/docs/[0]=={'foo2_s':['hi','there']}"
    );
  }

}


class BufferingRequestProcessor extends UpdateRequestProcessor
{
  List<AddUpdateCommand> addCommands = new ArrayList<AddUpdateCommand>();
  List<DeleteUpdateCommand> deleteCommands = new ArrayList<DeleteUpdateCommand>();
  List<CommitUpdateCommand> commitCommands = new ArrayList<CommitUpdateCommand>();
  List<RollbackUpdateCommand> rollbackCommands = new ArrayList<RollbackUpdateCommand>();
  
  public BufferingRequestProcessor(UpdateRequestProcessor next) {
    super(next);
  }
  
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    addCommands.add( cmd );
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    deleteCommands.add( cmd );
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    commitCommands.add( cmd );
  }
  
  @Override
  public void processRollback(RollbackUpdateCommand cmd) throws IOException
  {
    rollbackCommands.add( cmd );
  }

  @Override
  public void finish() throws IOException {
    // nothing?    
  }
}
