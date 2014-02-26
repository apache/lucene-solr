package org.apache.lucene.server;

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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONObject;

public class TestCustomDirectory extends ServerBaseTestCase {

  @BeforeClass
  public static void initClass() throws Exception {
    useDefaultIndex = false;
    startServer();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
  }

  private static boolean iWasUsed;

  public static class MyDirectory extends MMapDirectory {
    public MyDirectory(File path) throws IOException {
      super(path);
      iWasUsed = true;
    }
  }

  public void testCustomDirectory() throws Exception {
    curIndexName = "index";
    TestUtil.rmDir(new File("index"));
    send("createIndex", "{rootDir: " + curIndexName + "}");
    send("settings", "{directory: org.apache.lucene.server.TestCustomDirectory$MyDirectory, matchVersion: LUCENE_40}");
    send("startIndex");
    send("stopIndex");
    send("deleteIndex");
    assertTrue(iWasUsed);
  }

  public void testInvalidDirectory() throws Exception {
    curIndexName = "index";
    TestUtil.rmDir(new File("index"));
    send("createIndex", "{rootDir: " + curIndexName + "}");
    assertFailsWith("settings", "{directory: bad}", "could not locate Directory sub-class \"bad\"; verify CLASSPATH");
    send("deleteIndex");
  }
}
