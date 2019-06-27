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

package org.apache.solr.handler.admin;

import org.apache.solr.common.SolrException;
import org.junit.Test;

import static org.junit.Assert.*;

public class SolrEnvironmentTest {

  @Test(expected = SolrException.class)
  public void parseWrongKey() {
    SolrEnvironment.parse("foo");
  }

  @Test
  public void parsePredefined() {
    assertEquals("prod", SolrEnvironment.parse("prod").getCode());
    assertNull(SolrEnvironment.parse("prod").getColor());
    assertNull(SolrEnvironment.parse("prod").getLabel());

    assertEquals("stage", SolrEnvironment.parse("stage").getCode());
    assertNull(SolrEnvironment.parse("stage").getColor());
    assertNull(SolrEnvironment.parse("stage").getLabel());

    assertEquals("test", SolrEnvironment.parse("test").getCode());
    assertNull(SolrEnvironment.parse("test").getColor());
    assertNull(SolrEnvironment.parse("test").getLabel());

    assertEquals("dev", SolrEnvironment.parse("dev").getCode());
    assertNull(SolrEnvironment.parse("dev").getColor());
    assertNull(SolrEnvironment.parse("dev").getLabel());
  }

  @Test
  public void parseCustom() {
    assertEquals("my Label", SolrEnvironment.parse("prod,label=my+Label,color=blue").getLabel());
    assertEquals("blue", SolrEnvironment.parse("prod,label=my+Label,color=blue").getColor());
    assertEquals("my Label", SolrEnvironment.parse("prod,label=my+Label").getLabel());
    assertEquals("blue", SolrEnvironment.parse("prod,color=blue").getColor());
  }

  @Test(expected = SolrException.class)
  public void tryingToHackLabel() {
    SolrEnvironment.parse("prod,label=alert('hacked')");
  }

  @Test(expected = SolrException.class)
  public void tryingToHackColor() {
    SolrEnvironment.parse("prod,color=alert('hacked')");
  }

  @Test(expected = SolrException.class)
  public void illegalParam() {
    SolrEnvironment.parse("prod,foo=hello");
  }
}