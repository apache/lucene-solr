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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCoreDescriptor extends Assert {

  Path instanceDir;

  @Before
  public void setup() {
    instanceDir = Files.createTempDir().toPath();
  }


  @Test
  public void testNameIsNotOverrriddenByPropertiesMap() {
    Map<String, String> map = new HashMap();
    Properties properties = new Properties();
    properties.put(CoreDescriptor.CORE_NAME, "BadName");
    map.put(CoreDescriptor.CORE_NAME, "BadName");
    CoreDescriptor cd = new CoreDescriptor("GoodName", instanceDir, map, properties, false);
    assertTrue("Should not of allowed name to be overridden by properties",
        cd.getPersistableStandardProperties().getProperty(CoreDescriptor.CORE_NAME).equals("GoodName"));
  }
}
