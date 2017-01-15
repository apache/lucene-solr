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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.Period;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.util.SolrCLI.findTool;
import static org.apache.solr.util.SolrCLI.parseCmdLine;

/**
 * Unit test for SolrCLI's UtilsTool
 */
public class UtilsToolTest extends SolrTestCaseJ4 {

  private Path dir;
  private SolrCLI.UtilsTool tool;
  private List<String> files = Arrays.asList(
      "solr.log", 
      "solr.log.1", 
      "solr.log.2", 
      "solr.log.3", 
      "solr.log.9", 
      "solr.log.10", 
      "solr.log.11", 
      "solr_log_20160102", 
      "solr_log_20160304", 
      "solr-8983-console.log",
      "solr_gc_log_20160102", 
      "solr_gcnotremove", 
      "solr_gc.log", 
      "solr_gc.log.0", 
      "solr_gc.log.0.current", 
      "solr_gc_log_2");
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    dir = createTempDir("Utils Tool Test").toAbsolutePath();
    files.forEach(f -> {
      try {
        Files.createFile(dir.resolve(f));
      } catch (IOException e) {
        fail("Error when creating temporary file " + dir.resolve(f));
      }
    });
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    org.apache.commons.io.FileUtils.deleteDirectory(dir.toFile());
  }
  
  @Test
  public void testEmptyAndQuiet() throws Exception {
    String[] args = {"utils", "-remove_old_solr_logs", "7",  
        "-rotate_solr_logs", "9",  
        "-archive_gc_logs",
        "-archive_console_logs",
        "-q",
        "-l", dir.toString()};
    assertEquals(0, runTool(args));
  }

  @Test
  public void testNonexisting() throws Exception {
    String nonexisting = dir.resolve("non-existing").toString();
    String[] args = {"utils", "-remove_old_solr_logs", "7",
        "-rotate_solr_logs", "9",
        "-archive_gc_logs",
        "-archive_console_logs",
        "-l", nonexisting};
    assertEquals(0, runTool(args));
  }
  
  @Test
  public void testRemoveOldSolrLogs() throws Exception {
    String[] args = {"utils", "-remove_old_solr_logs", "1", "-l", dir.toString()};
    assertEquals(files.size(), fileCount());
    assertEquals(0, runTool(args));
    assertEquals(files.size(), fileCount());     // No logs older than 1 day
    Files.setLastModifiedTime(dir.resolve("solr_log_20160102"), FileTime.from(Instant.now().minus(Period.ofDays(2))));
    assertEquals(0, runTool(args));
    assertEquals(files.size()-1, fileCount());   // One logs older than 1 day
    Files.setLastModifiedTime(dir.resolve("solr_log_20160304"), FileTime.from(Instant.now().minus(Period.ofDays(3))));
    assertEquals(0, runTool(args));
    assertEquals(files.size()-2, fileCount());   // Two logs older than 1 day
  }

  @Test
  public void testRelativePath() throws Exception {
    String[] args = {"utils", "-remove_old_solr_logs", "0", "-l", dir.getFileName().toString(), "-s", dir.getParent().toString()};
    assertEquals(files.size(), fileCount());
    assertEquals(0, runTool(args));
    assertEquals(files.size()-2, fileCount());
  }

  @Test
  public void testRelativePathError() throws Exception {
    String[] args = {"utils", "-remove_old_solr_logs", "0", "-l", dir.getFileName().toString()};
    try {
      runTool(args);
    } catch (Exception e) {
      return;
    }
    fail("Should have thrown exception if using relative path without -s");
  }
  
  @Test
  public void testRemoveOldGcLogs() throws Exception {
    String[] args = {"utils", "-archive_gc_logs", "-l", dir.toString()};
    assertEquals(files.size(), fileCount());
    assertEquals(0, runTool(args));
    assertEquals(files.size()-5, fileCount());
    assertFalse(listFiles().contains("solr_gc_log_2"));
    assertTrue(Files.exists(dir.resolve("archived").resolve("solr_gc_log_2")));
    assertEquals(0, runTool(args));
    assertFalse(Files.exists(dir.resolve("archived").resolve("solr_gc_log_2")));
  }

  @Test
  public void testArchiveConsoleLogs() throws Exception {
    String[] args = {"utils", "-archive_console_logs", "-l", dir.toString()};
    assertEquals(files.size(), fileCount());
    assertEquals(0, runTool(args));
    assertEquals(files.size()-1, fileCount());
    assertFalse(listFiles().contains("solr-8983-console.log"));
    assertTrue(Files.exists(dir.resolve("archived").resolve("solr-8983-console.log")));
    assertEquals(0, runTool(args));
    assertFalse(Files.exists(dir.resolve("archived").resolve("solr-8983-console.log")));
  }

  @Test
  public void testRotateSolrLogs() throws Exception {
    String[] args = {"utils", "-rotate_solr_logs", "9", "-l", dir.toString()};
    assertEquals(files.size(), fileCount());
    assertTrue(listFiles().contains("solr.log"));
    assertEquals(0, runTool(args));
    assertEquals(files.size()-3, fileCount());
    assertTrue(listFiles().contains("solr.log.4"));
    assertFalse(listFiles().contains("solr.log"));
    assertFalse(listFiles().contains("solr.log.9"));
    assertFalse(listFiles().contains("solr.log.10"));
    assertFalse(listFiles().contains("solr.log.11"));
  }
  
  private List<String> listFiles() throws IOException {
    return Files.find(dir, 1, (p, a) -> a.isRegularFile()).map(p -> p.getFileName().toString()).collect(Collectors.toList());
  }
  
  private long fileCount() throws IOException {
    return listFiles().size();
  }

  private int runTool(String[] args) throws Exception {
    SolrCLI.Tool tool = findTool(args);
    CommandLine cli = parseCmdLine(args, tool.getOptions());
    return tool.runTool(cli);
  }
}