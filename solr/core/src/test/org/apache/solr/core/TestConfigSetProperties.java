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
package org.apache.solr.core;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import com.google.common.collect.ImmutableMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class TestConfigSetProperties extends SolrTestCaseJ4 {

  @Rule
  public TestRule testRule = RuleChain.outerRule(new SystemPropertiesRestoreRule());
  

  @Test
  public void testNoConfigSetPropertiesFile() throws Exception {
    assertNull(createConfigSetProps(null));
  }

  @Test
  public void testEmptyConfigSetProperties() throws Exception {
    SolrException thrown = expectThrows(SolrException.class, () -> {
      createConfigSetProps("");
    });
    assertEquals(ErrorCode.SERVER_ERROR.code, thrown.code());
  }

  @Test
  public void testConfigSetPropertiesNotMap() throws Exception {
    SolrException thrown = expectThrows(SolrException.class, () -> {
      createConfigSetProps(Utils.toJSONString(new String[] {"test"}));
    });
    assertEquals(ErrorCode.SERVER_ERROR.code, thrown.code());
  }

  @Test
  public void testEmptyMap() throws Exception {
    @SuppressWarnings({"rawtypes"})
    NamedList list = createConfigSetProps(Utils.toJSONString(ImmutableMap.of()));
    assertEquals(0, list.size());
  }

  @Test
  public void testMultipleProps() throws Exception {
    @SuppressWarnings({"rawtypes"})
    Map map = ImmutableMap.of("immutable", "true", "someOtherProp", "true");
    @SuppressWarnings({"rawtypes"})
    NamedList list = createConfigSetProps(Utils.toJSONString(map));
    assertEquals(2, list.size());
    assertEquals("true", list.get("immutable"));
    assertEquals("true", list.get("someOtherProp"));
  }

  @SuppressWarnings({"rawtypes"})
  private NamedList createConfigSetProps(String props) throws Exception {
    Path testDirectory = createTempDir();
    String filename = "configsetprops.json";
    if (props != null) {
      Path confDir = testDirectory.resolve("conf");
      Files.createDirectories(confDir);
      Files.write(confDir.resolve(filename), props.getBytes(StandardCharsets.UTF_8));
    }
    SolrResourceLoader loader = new SolrResourceLoader(testDirectory);
    return ConfigSetProperties.readFromResourceLoader(loader, filename);
  }
}
