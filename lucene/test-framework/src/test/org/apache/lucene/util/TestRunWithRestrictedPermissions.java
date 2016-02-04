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
package org.apache.lucene.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.AllPermission;

public class TestRunWithRestrictedPermissions extends LuceneTestCase {

  public void testDefaultsPass() throws Exception {
    runWithRestrictedPermissions(this::doSomeForbiddenStuff, new AllPermission());
  }

  public void testNormallyAllowedStuff() throws Exception {
    try {
      runWithRestrictedPermissions(this::doSomeForbiddenStuff);
      fail("this should not pass!");
    } catch (SecurityException se) {
      // pass
    }
  }

  public void testCompletelyForbidden1() throws Exception {
    try {
      runWithRestrictedPermissions(this::doSomeCompletelyForbiddenStuff);
      fail("this should not pass!");
    } catch (SecurityException se) {
      // pass
    }
  }

  public void testCompletelyForbidden2() throws Exception {
    try {
      runWithRestrictedPermissions(this::doSomeCompletelyForbiddenStuff, new AllPermission());
      fail("this should not pass (not even with AllPermission)");
    } catch (SecurityException se) {
      // pass
    }
  }

  private Void doSomeForbiddenStuff() throws IOException {
    createTempDir("cannot_create_temp_folder");
    return null; // Void
  }
  
  // something like this should not never pass!!
  private Void doSomeCompletelyForbiddenStuff() throws IOException {
    Files.createFile(Paths.get("denied"));
    return null; // Void
  }
  
}
