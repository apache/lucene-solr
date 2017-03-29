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

package org.apache.solr.util.pf4j;

import java.util.List;

import org.junit.Test;
import ro.fortsoft.pf4j.update.UpdateRepository;

import static junit.framework.Assert.assertEquals;

/**
 * Created by janhoy on 29.03.2017.
 */
public class Pf4jPluginsTest {
  @Test
  public void query() throws Exception {
    // NOCOMMIT: Get rid of GSON dependency
    Pf4jPlugins mgr = new Pf4jPlugins();
    List<UpdateRepository.PluginInfo> res = mgr.query("*");
    assertEquals(1, res.size());
    assertEquals("extraction", res.get(0).id);

    assertEquals(1, mgr.query("extract").size());
  }

}