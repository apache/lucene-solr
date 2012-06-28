package org.apache.lucene.benchmark;

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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;

import org.apache.lucene.benchmark.byTask.Benchmark;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Base class for all Benchmark unit tests. */
public abstract class BenchmarkTestCase extends LuceneTestCase {
  private static File WORKDIR;
  
  @BeforeClass
  public static void beforeClassBenchmarkTestCase() {
    WORKDIR = _TestUtil.getTempDir("benchmark");
    WORKDIR.delete();
    WORKDIR.mkdirs();
  }
  
  @AfterClass
  public static void afterClassBenchmarkTestCase() {
    WORKDIR = null;
  }
  
  public File getWorkDir() {
    return WORKDIR;
  }
  
  /** Copy a resource into the workdir */
  public void copyToWorkDir(String resourceName) throws IOException {
    InputStream resource = getClass().getResourceAsStream(resourceName);
    OutputStream dest = new FileOutputStream(new File(getWorkDir(), resourceName));
    byte[] buffer = new byte[8192];
    int len;
    
    while ((len = resource.read(buffer)) > 0) {
        dest.write(buffer, 0, len);
    }

    resource.close();
    dest.close();
  }
  
  /** Return a path, suitable for a .alg config file, for a resource in the workdir */
  public String getWorkDirResourcePath(String resourceName) {
    return new File(getWorkDir(), resourceName).getAbsolutePath().replace("\\", "/");
  }
  
  /** Return a path, suitable for a .alg config file, for the workdir */
  public String getWorkDirPath() {
    return getWorkDir().getAbsolutePath().replace("\\", "/");
  }
  
  // create the benchmark and execute it. 
  public Benchmark execBenchmark(String[] algLines) throws Exception {
    String algText = algLinesToText(algLines);
    logTstLogic(algText);
    Benchmark benchmark = new Benchmark(new StringReader(algText));
    benchmark.execute();
    return benchmark;
  }
  
  // properties in effect in all tests here
  final String propLines [] = {
    "work.dir=" + getWorkDirPath(),
    "directory=RAMDirectory",
    "print.props=false",
  };
  
  static final String NEW_LINE = System.getProperty("line.separator");
  
  // catenate alg lines to make the alg text
  private String algLinesToText(String[] algLines) {
    String indent = "  ";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < propLines.length; i++) {
      sb.append(indent).append(propLines[i]).append(NEW_LINE);
    }
    for (int i = 0; i < algLines.length; i++) {
      sb.append(indent).append(algLines[i]).append(NEW_LINE);
    }
    return sb.toString();
  }

  private static void logTstLogic (String txt) {
    if (!VERBOSE) 
      return;
    System.out.println("Test logic of:");
    System.out.println(txt);
  }

}
