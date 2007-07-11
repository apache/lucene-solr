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

package org.apache.solr.update;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.XmlUpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.AbstractSolrTestCase;

/**
 * 
 *
 */
public class DirectUpdateHandlerTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  

  public void testRequireUniqueKey() throws Exception 
  {
    SolrCore core = SolrCore.getSolrCore();
    
    UpdateHandler updater = core.getUpdateHandler();
    
    AddUpdateCommand cmd = new AddUpdateCommand();
    cmd.overwriteCommitted = true;
    cmd.overwritePending = true;
    cmd.allowDups = false;
    
    // Add a valid document
    cmd.doc = new Document();
    cmd.doc.add( new Field( "id", "AAA", Store.YES, Index.UN_TOKENIZED ) );
    cmd.doc.add( new Field( "subject", "xxxxx", Store.YES, Index.UN_TOKENIZED ) );
    updater.addDoc( cmd );
    
    // Add a document with multiple ids
    cmd.indexedId = null;  // reset the id for this add
    cmd.doc = new Document();
    cmd.doc.add( new Field( "id", "AAA", Store.YES, Index.UN_TOKENIZED ) );
    cmd.doc.add( new Field( "id", "BBB", Store.YES, Index.UN_TOKENIZED ) );
    cmd.doc.add( new Field( "subject", "xxxxx", Store.YES, Index.UN_TOKENIZED ) );
    try {
      updater.addDoc( cmd );
      fail( "added a document with multiple ids" );
    }
    catch( SolrException ex ) { } // expected

    // Add a document without an id
    cmd.indexedId = null;  // reset the id for this add
    cmd.doc = new Document();
    cmd.doc.add( new Field( "subject", "xxxxx", Store.YES, Index.UN_TOKENIZED ) );
    try {
      updater.addDoc( cmd );
      fail( "added a document without an ids" );
    }
    catch( SolrException ex ) { } // expected
  }
  
}
