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

import org.apache.solr.SolrTestCaseJ4;

public class FileTypeMagicUtilTest extends SolrTestCaseJ4 {
  public void testGuessMimeType() {
    assertEquals(
        "application/x-java-applet",
        FileTypeMagicUtil.INSTANCE.guessMimeType(
            FileTypeMagicUtil.class.getResourceAsStream("/magic/HelloWorldJavaClass.class.bin")));
    assertEquals(
        "application/zip",
        FileTypeMagicUtil.INSTANCE.guessMimeType(
            FileTypeMagicUtil.class.getResourceAsStream(
                "/runtimecode/containerplugin.v.1.jar.bin")));
    assertEquals(
        "application/x-tar",
        FileTypeMagicUtil.INSTANCE.guessMimeType(
            FileTypeMagicUtil.class.getResourceAsStream("/magic/hello.tar.bin")));
    assertEquals(
        "text/x-shellscript",
        FileTypeMagicUtil.INSTANCE.guessMimeType(
            FileTypeMagicUtil.class.getResourceAsStream("/magic/shell.sh.txt")));
  }

  public void testIsFileForbiddenInConfigset() {
    assertTrue(
        FileTypeMagicUtil.isFileForbiddenInConfigset(
            FileTypeMagicUtil.class.getResourceAsStream("/magic/HelloWorldJavaClass.class.bin")));
    assertTrue(
        FileTypeMagicUtil.isFileForbiddenInConfigset(
            FileTypeMagicUtil.class.getResourceAsStream("/magic/shell.sh.txt")));
    assertFalse(
        FileTypeMagicUtil.isFileForbiddenInConfigset(
            FileTypeMagicUtil.class.getResourceAsStream("/magic/plain.txt")));
  }
}
