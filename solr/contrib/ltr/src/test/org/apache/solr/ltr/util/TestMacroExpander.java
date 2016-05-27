package org.apache.solr.ltr.util;

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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TestMacroExpander {

  @Test
  public void testEmptyExpander() {
    Map<String,String> efi = new HashMap<String,String>();
    MacroExpander macroExpander = new MacroExpander(efi);

    assertEquals("", macroExpander.expand(""));
    assertEquals("foo", macroExpander.expand("foo"));
    assertEquals("$foo", macroExpander.expand("$foo"));
    assertEquals("${foo}", macroExpander.expand("${foo}"));
    assertEquals("{foo}", macroExpander.expand("{foo}"));
    assertEquals("${foo}", MacroExpander.expand("${foo}", efi));
  }

  @Test
  public void testExpander() {
    Map<String,String> efi = new HashMap<String,String>();
    efi.put("foo", "bar");
    efi.put("baz", "bat");
    MacroExpander macroExpander = new MacroExpander(efi);

    assertEquals("", macroExpander.expand(""));
    assertEquals("foo", macroExpander.expand("foo"));
    assertEquals("$foo", macroExpander.expand("$foo"));
    assertEquals("bar", macroExpander.expand("${foo}"));
    assertEquals("{foo}", macroExpander.expand("{foo}"));
    assertEquals("bar", MacroExpander.expand("${foo}", efi));
    assertEquals("foo bar baz bat",
        macroExpander.expand("foo ${foo} baz ${baz}"));
  }
}
