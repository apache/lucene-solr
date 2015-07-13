package org.apache.solr.core;

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

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.noggit.JSONUtil;

import java.io.File;

public class TestConfigSetProperties extends SolrTestCaseJ4 {

  @Rule
  public TestRule testRule = RuleChain.outerRule(new SystemPropertiesRestoreRule());
  

  @Test
  public void testNoConfigSetPropertiesFile() throws Exception {
    assertNull(createConfigSetProps(null));
  }

  @Test
  public void testEmptyConfigSetProperties() throws Exception {
    try {
      createConfigSetProps("");
      fail("Excepted SolrException");
    } catch (SolrException ex) {
      assertEquals(ErrorCode.SERVER_ERROR.code, ex.code());
    }
  }

  @Test
  public void testConfigSetPropertiesNotMap() throws Exception {
    try {
      createConfigSetProps(JSONUtil.toJSON(new String[] {"test"}));
      fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(ErrorCode.SERVER_ERROR.code, ex.code());
    }
  }

  @Test
  public void testEmptyMap() throws Exception {
    NamedList list = createConfigSetProps(JSONUtil.toJSON(ImmutableMap.of()));
    assertEquals(0, list.size());
  }

  @Test
  public void testMultipleProps() throws Exception {
    Map map = ImmutableMap.of("immutable", "true", "someOtherProp", "true");
    NamedList list = createConfigSetProps(JSONUtil.toJSON(map));
    assertEquals(2, list.size());
    assertEquals("true", list.get("immutable"));
    assertEquals("true", list.get("someOtherProp"));
  }

  private NamedList createConfigSetProps(String props) throws Exception {
    File testDirectory = createTempDir().toFile();
    String filename = "configsetprops.json";
    if (props != null) {
      File confDir = new File(testDirectory, "conf");
      FileUtils.forceMkdir(confDir);
      FileUtils.write(new File(confDir, filename), new StringBuilder(props));
    }
    SolrResourceLoader loader = new SolrResourceLoader(testDirectory.getAbsolutePath());
    return ConfigSetProperties.readFromResourceLoader(loader, filename);
  }
}
