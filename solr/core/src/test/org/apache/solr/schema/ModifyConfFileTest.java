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

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.util.ArrayList;

public class ModifyConfFileTest extends SolrTestCaseJ4 {
  private File solrHomeDirectory = new File(TEMP_DIR, this.getClass().getName());

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  private CoreContainer init() throws Exception {
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());

    copySolrHomeToTemp(solrHomeDirectory, "core1", true);
    FileUtils.write(new File(new File(solrHomeDirectory, "core1"), "core.properties"), "", Charsets.UTF_8.toString());
    final CoreContainer cores = new CoreContainer(solrHomeDirectory.getAbsolutePath());
    cores.load();
    return cores;
  }

  @Test
  public void testConfigWrite() throws Exception {

    final CoreContainer cc = init();
    try {
      //final CoreAdminHandler admin = new CoreAdminHandler(cc);

      SolrCore core = cc.getCore("core1");
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestHandler handler = core.getRequestHandler("/admin/fileedit");

      ModifiableSolrParams params = params("file","schema.xml", "op","write");
      core.execute(handler, new LocalSolrQueryRequest(core, params), rsp);
      assertEquals(rsp.getException().getMessage(), "Input stream list was null for admin file write operation.");

      params = params("op", "write");
      core.execute(handler, new LocalSolrQueryRequest(core, params), rsp);
      assertEquals(rsp.getException().getMessage(), "No file name specified for write operation.");

      ArrayList<ContentStream> streams = new ArrayList<ContentStream>( 2 );
      streams.add(new ContentStreamBase.StringStream("Testing rewrite of schema.xml file." ) );

      params = params("op", "write", "file", "bogus.txt");
      LocalSolrQueryRequest locReq = new LocalSolrQueryRequest(core, params);
      locReq.setContentStreams(streams);
      core.execute(handler, locReq, rsp);
      assertEquals(rsp.getException().getMessage(), "Can not access: bogus.txt");

      String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
      String badConf = FileUtils.readFileToString(new File(top, "solrconfig-minimal.xml"), "UTF-8").replace("</dataDir>", "");

      params = params("op", "write", "file", "solrconfig.xml");
      locReq = new LocalSolrQueryRequest(core, params);
      streams.clear();
      streams.add(new ContentStreamBase.StringStream(badConf));
      locReq.setContentStreams(streams);
      core.execute(handler, locReq, rsp);
      assertTrue("should have detected an error early!",
          rsp.getException().getMessage().contains("\"dataDir\""));

      assertTrue("should have detected an error early!",
          rsp.getException().getMessage().contains("\"</dataDir>\""));

      params = params("op", "test", "file", "schema.xml", "stream.body", "Testing rewrite of schema.xml file.");
      locReq = new LocalSolrQueryRequest(core, params);
      locReq.setContentStreams(streams);
      core.execute(handler, locReq, rsp);

      assertTrue("Schema should have caused core reload to fail!",
          rsp.getException().getMessage().indexOf("SAXParseException") != -1);
      String contents = FileUtils.readFileToString(new File(core.getCoreDescriptor().getInstanceDir(), "conf/schema.xml"), Charsets.UTF_8.toString());
      assertFalse("Schema contents should NOT have changed!", contents.contains("Testing rewrite of schema.xml file."));

      streams.add(new ContentStreamBase.StringStream("This should barf"));
      locReq = new LocalSolrQueryRequest(core, params);
      locReq.setContentStreams(streams);
      core.execute(handler, locReq, rsp);
      assertEquals(rsp.getException().getMessage(), "More than one input stream was found for admin file write operation.");

      streams.clear();
      streams.add(new ContentStreamBase.StringStream("Some bogus stuff for a test."));
      params = params("op", "write", "file", "velocity/test.vm");
      locReq = new LocalSolrQueryRequest(core, params);
      locReq.setContentStreams(streams);
      core.execute(handler, locReq, rsp);
      contents = FileUtils.readFileToString(new File(core.getCoreDescriptor().getInstanceDir(),
          "conf/velocity/test.vm"), Charsets.UTF_8.toString());
      assertEquals("Schema contents should have changed!", "Some bogus stuff for a test.", contents);

      streams.clear();
      params = params();
      locReq = new LocalSolrQueryRequest(core, params);

      core.execute(core.getRequestHandler("/admin/file"), locReq, rsp);

      NamedList<Object> res = rsp.getValues();

      NamedList files = (NamedList)res.get("files");
      assertNotNull("Should have gotten files back", files);
      SimpleOrderedMap schema = (SimpleOrderedMap)files.get("schema.xml");
      assertNotNull("Should have a schema returned", schema);
      assertNull("Schema.xml should not be a directory", schema.get("directory"));

      SimpleOrderedMap velocity = (SimpleOrderedMap)files.get("velocity");
      assertNotNull("Should have velocity dir returned", velocity);

      assertTrue("Velocity should be a directory", Boolean.parseBoolean(velocity.get("directory").toString()));

      core.close();
    } finally {
      cc.shutdown();
      if (solrHomeDirectory.exists()) {
        FileUtils.deleteDirectory(solrHomeDirectory);
      }
    }

  }
}
