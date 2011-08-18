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

package org.apache.solr.handler.admin;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.response.SolrQueryResponse;

import java.io.File;
import java.io.IOException;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

public class CoreAdminHandlerTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }
  
  @Test
  public void testCoreAdminHandler() throws Exception {
    final File workDir = new File(TEMP_DIR, this.getClass().getName());

    if (workDir.exists()) {
      FileUtils.deleteDirectory(workDir);
    }
    assertTrue("Failed to mkdirs workDir", workDir.mkdirs());
    
    final CoreContainer cores = h.getCoreContainer();
    cores.setPersistent(false); // we'll do this explicitly as needed

    final CoreAdminHandler admin = new CoreAdminHandler(cores);

    String instDir = null;
    {
      SolrCore template = null;
      try {
        template = cores.getCore("collection1");
        instDir = template.getCoreDescriptor().getInstanceDir();
      } finally {
        if (null != template) template.close();
      }
    }
    
    final File instDirFile = new File(instDir);
    assertTrue("instDir doesn't exist: " + instDir, instDirFile.exists());
    final File instPropFile = new File(workDir, "instProp");
    FileUtils.copyDirectory(instDirFile, instPropFile);
    
    // create a new core (using CoreAdminHandler) w/ properties
    
    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody
      (req(CoreAdminParams.ACTION, 
           CoreAdminParams.CoreAdminAction.CREATE.toString(),
           CoreAdminParams.INSTANCE_DIR, instPropFile.getAbsolutePath(),
           CoreAdminParams.NAME, "props",
           CoreAdminParams.PROPERTY_PREFIX + "hoss","man",
           CoreAdminParams.PROPERTY_PREFIX + "foo","baz"),
       resp);
    assertNull("Exception on create", resp.getException());

    // verify props are in persisted file

    final File xml = new File(workDir, "persist-solr.xml");
    cores.persistFile(xml);
    
    assertXmlFile
      (xml
       ,"/solr/cores/core[@name='props']/property[@name='hoss' and @value='man']"
       ,"/solr/cores/core[@name='props']/property[@name='foo' and @value='baz']"
       );
    
  }

  
  public void assertXmlFile(final File file, String... xpath)
      throws IOException, SAXException {
    
    try {
      String xml = FileUtils.readFileToString(file, "UTF-8");
      String results = h.validateXPath(xml, xpath);
      if (null != results) {
        String msg = "File XPath failure: file=" + file.getPath() + " xpath="
            + results + "\n\nxml was: " + xml;
        fail(msg);
      }
    } catch (XPathExpressionException e2) {
      throw new RuntimeException("XPath is invalid", e2);
    }
  }
  
}
