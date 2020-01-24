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

package org.apache.lucene.luke.app.desktop.util.inifile;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class SimpleIniFileTest extends LuceneTestCase {

  @Test
  public void testStore() throws IOException {
    Path path = saveTestIni();
    assertTrue(Files.exists(path));
    assertTrue(Files.isRegularFile(path));

    try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      List<String> lines = br.lines().collect(Collectors.toList());
      assertEquals(8, lines.size());
      assertEquals("[section1]", lines.get(0));
      assertEquals("s1 = aaa", lines.get(1));
      assertEquals("s2 = bbb", lines.get(2));
      assertEquals("", lines.get(3));
      assertEquals("[section2]", lines.get(4));
      assertEquals("b1 = true", lines.get(5));
      assertEquals("b2 = false", lines.get(6));
      assertEquals("", lines.get(7));
    }
  }

  @Test
  public void testLoad() throws IOException {
    Path path = saveTestIni();

    SimpleIniFile iniFile = new SimpleIniFile();
    iniFile.load(path);

    Map<String, OptionMap> sections = iniFile.getSections();
    assertEquals(2, sections.size());
    assertEquals(2, sections.get("section1").size());
    assertEquals(2, sections.get("section2").size());
  }

  @Test
  public void testPut() {
    SimpleIniFile iniFile = new SimpleIniFile();
    iniFile.put("section1", "s1", "aaa");
    iniFile.put("section1", "s1", "aaa_updated");
    iniFile.put("section2", "b1", true);
    iniFile.put("section2", "b2", null);

    Map<String, OptionMap> sections = iniFile.getSections();
    assertEquals("aaa_updated", sections.get("section1").get("s1"));
    assertEquals("true", sections.get("section2").get("b1"));
    assertNull(sections.get("section2").get("b2"));
  }

  @Test
  public void testGet() throws IOException {
    Path path = saveTestIni();
    SimpleIniFile iniFile = new SimpleIniFile();
    iniFile.load(path);

    assertNull(iniFile.getString("", ""));

    assertEquals("aaa", iniFile.getString("section1", "s1"));
    assertEquals("bbb", iniFile.getString("section1", "s2"));
    assertNull(iniFile.getString("section1", "s3"));
    assertNull(iniFile.getString("section1", ""));

    assertEquals(true, iniFile.getBoolean("section2", "b1"));
    assertEquals(false, iniFile.getBoolean("section2", "b2"));
    assertFalse(iniFile.getBoolean("section2", "b3"));
  }

  private Path saveTestIni() throws IOException {
    SimpleIniFile iniFile = new SimpleIniFile();
    iniFile.put("", "s0", "000");

    iniFile.put("section1", "s1", "aaa");
    iniFile.put("section1", "s2", "---");
    iniFile.put("section1", "s2", "bbb");
    iniFile.put("section1", "", "ccc");

    iniFile.put("section2", "b1", true);
    iniFile.put("section2", "b2", false);

    Path path = createTempFile();
    iniFile.store(path);
    return path;
  }
}
