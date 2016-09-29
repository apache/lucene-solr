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

package org.apache.solr.util;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test the post tool
 */
public class PostCLITest {
  private PostCLI cli;

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void runTool() throws Exception {

  }

  @Test
  public void options() throws Exception {
    assertEquals(11, PostCLI.options().getOptions().size());
  }

  @Test
  public void main() throws Exception {
    assertEquals(11, PostCLI.options().getOptions().size());
  }
}