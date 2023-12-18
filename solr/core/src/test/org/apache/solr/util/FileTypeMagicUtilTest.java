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

import java.io.IOException;
import java.io.InputStream;
import org.apache.solr.SolrTestCaseJ4;

public class FileTypeMagicUtilTest extends SolrTestCaseJ4 {
  public void testGuessMimeType() throws IOException {
    assertResourceMimeType("application/x-java-applet", "/magic/HelloWorldJavaClass.class.bin");
    assertResourceMimeType("application/zip", "/runtimecode/containerplugin.v.1.jar.bin");
    assertResourceMimeType("application/x-tar", "/magic/hello.tar.bin");
    assertResourceMimeType("text/x-shellscript", "/magic/shell.sh.txt");
  }

  public void testIsFileForbiddenInConfigset() throws IOException {
    assertResourceForbiddenInConfigset("/magic/HelloWorldJavaClass.class.bin");
    assertResourceForbiddenInConfigset("/magic/shell.sh.txt");
    assertResourceAllowedInConfigset("/magic/plain.txt");
  }

  private void assertResourceMimeType(String mimeType, String resourcePath) throws IOException {
    try (InputStream stream =
        FileTypeMagicUtil.class.getResourceAsStream("/magic/HelloWorldJavaClass.class.bin")) {
      assertEquals("application/x-java-applet", FileTypeMagicUtil.INSTANCE.guessMimeType(stream));
    }
  }

  private void assertResourceForbiddenInConfigset(String resourcePath) throws IOException {
    try (InputStream stream = FileTypeMagicUtil.class.getResourceAsStream(resourcePath)) {
      assertTrue(FileTypeMagicUtil.isFileForbiddenInConfigset(stream));
    }
  }

  private void assertResourceAllowedInConfigset(String resourcePath) throws IOException {
    try (InputStream stream = FileTypeMagicUtil.class.getResourceAsStream(resourcePath)) {
      assertFalse(FileTypeMagicUtil.isFileForbiddenInConfigset(stream));
    }
  }
}
