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
import static org.junit.Assume.assumeTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class BaseTestClass extends Assert {
  
  protected static Map<String,String> env = new HashMap<>(System.getenv());
  static {
    env.put("CONTAINER_NAME", "lucenesolr-build-test");
  }
  
  public BaseTestClass() {

  }
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String[] cmd = new String[]{"bash", "-c", "command -v docker >/dev/null 2>&1 || { echo \"docker must be installed to run this test\"; exit 1; }"};
    int exitVal = runCmd(cmd, env, false, false).returnCode;
    System.out.println("Exit:" + exitVal);
    assumeTrue(exitVal == 0);
    
    cmd = new String[]{"bash", "test-build-wdocker/start.sh"};
    exitVal = runCmd(cmd, env, false, false).returnCode;
    if (exitVal > 0) {
      fail("Failed starting docker containers!");
    }
    
    // hack to allows us to run gradle in the host and then in docker on the same project files
    cmd = new String[]{"bash",  "-c", "docker exec --user ${UID} -t ${CONTAINER_NAME} bash -c \"rm -rf /home/lucene/project/.gradle\""};
    exitVal = runCmd(cmd, env, false, false).returnCode;
    if (exitVal > 0) {
      //fail("Failed removing .gradle dir!");
    }
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception { 
    String[] cmd = new String[] {"bash",
        "test-build-wdocker/stop.sh"};
    int exitVal = runCmd(cmd, env, false, false).returnCode;
    if (exitVal > 0) {
      fail("Failed stopping docker containers!");
    }
  }
  
  @Before
  public void setUp() throws Exception {

  }
  
  @After
  public void tearDown() throws Exception {

  }
  
  public static PbResult runCmd(String[] cmd, boolean cmdIsNoop, boolean trace)
      throws Exception {
    return runCmd(cmd, Collections.emptyMap(), cmdIsNoop, trace);
  }
  
  public static PbResult runCmd(String[] cmd, Map<String,String> env,
      boolean cmdIsNoop, boolean trace) throws Exception {
    return runCmd(cmd, env, null, cmdIsNoop, trace);
  }
  
  public static PbResult runCmd(String[] cmd, Map<String,String> env, File cwd,
      boolean cmdIsNoop, boolean trace) throws Exception {
    if (trace) System.out.println("execute cmd: " + Arrays.asList(cmd));
    if (cmdIsNoop) return new PbResult();
    ProcessBuilder pb = new ProcessBuilder(cmd);
    return processPb(env, pb, false);
  }
  
  public static PbResult runCmd(String cmd, Map<String,String> env,
      boolean cmdIsNoop, boolean trace) throws Exception {
    return runCmd(cmd, env, cmdIsNoop, trace);
  }
  
  public static PbResult runCmd(String cmd, Map<String,String> env,
      boolean cmdIsNoop, boolean trace, boolean returnOutput) throws Exception {
    if (trace) System.out.println("execute cmd: " + cmd);
    if (cmdIsNoop) return new PbResult();
    ProcessBuilder pb = new ProcessBuilder(cmd);
    return processPb(env, pb, returnOutput);
  }
  
  public static PbResult runCmd(String[] cmd, Map<String,String> env,
      boolean cmdIsNoop, boolean trace, boolean returnOutput) throws Exception {
    return runCmd(cmd, env, null, cmdIsNoop, trace, returnOutput);
  }
  
  public static PbResult runCmd(String[] cmd, Map<String,String> env, File cwd,
      boolean cmdIsNoop, boolean trace, boolean returnOutput) throws Exception {
    if (trace) System.out.println("execute cmd: " + Arrays.asList(cmd));
    if (cmdIsNoop) return new PbResult();
    ProcessBuilder pb = new ProcessBuilder(cmd);
    return processPb(env, pb, cwd, returnOutput);
  }
  
  private static PbResult processPb(Map<String,String> env, ProcessBuilder pb, boolean returnOutput)
      throws Exception {
    return processPb(env, pb, null, returnOutput);
  }
  
  private static PbResult processPb(Map<String,String> env, ProcessBuilder pb, File cwd, boolean returnOutput)
      throws Exception {
    try {
      pb.environment().putAll(env);
      pb.environment().put("PATH", System.getenv().get("PATH") + File.pathSeparator + "/usr/local/bin" + File.pathSeparator + "/usr/bin");
      System.out.println(System.getenv());
     // pb.environment().put("UID", System.getenv().get("UID"));
    } catch (Exception e) {
      System.out.println("Error setting env variables: " + env);
      throw e;
    }
    pb.redirectErrorStream(true);
    if (cwd == null) {
      pb.directory(cwd);
    }
    pb.inheritIO();
    Process p = null;
    try {
      p = pb.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
    StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = br.readLine()) != null) {
      System.out.println(line);
      if (returnOutput) {
        sb.append(line);
      }
    }
    
    // System.out.println(builder.toString());
    int returnCode = p.waitFor();
    PbResult result = new PbResult();
    result.returnCode = returnCode;
    result.output = sb.toString();
    return result;
  }
  
  public static class PbResult {
    int returnCode;
    String output;
  }

}
