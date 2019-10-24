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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestChecks extends BaseTestClass {
  
  public TestChecks() {

  }
  
  @Before
  public void setUp() throws Exception {
    cleanTestFiles();
  }
  
  @After
  public void tearDown() throws Exception {
    cleanTestFiles();
  }

  private void cleanTestFiles() throws Exception {
    String[] cmd = new String[]{"bash", "-c", "docker exec --user ${UID} -t ${CONTAINER_NAME} bash -c \"rm /home/lucene/project/solr/core/src/test/org/no_license_test_file.java 2>/dev/null \""};
    runCmd(cmd, env, false, false);
    
    cmd = new String[]{"bash", "-c", "docker exec --user ${UID} -t ${CONTAINER_NAME} bash -c \"rm /home/lucene/project/lucene/core/src/java/org/no_license_test_file.xml 2>/dev/null \""};
    runCmd(cmd, env, false, false);
    
    
    cmd = new String[]{"bash", "-c", "docker exec --user ${UID} -t ${CONTAINER_NAME} bash -c \"rm /home/lucene/project/solr/contrib/clustering/src/java/org/tab_file.xml 2>/dev/null \""};
    runCmd(cmd, env, false, false);
  }
  
  @Test
  public void testRatSources() throws Exception {
    System.out.println("Start test-rat-sources.sh test in Docker container (" + env + ") ...");
    String[] cmd = new String[]{"bash", "src/buildTest/scripts/test-rat-sources.sh", "-r", resultFile};
    PbResult result = runCmd(cmd, env, false, false, false);
    
    String msg = "";
    
    if (Files.exists(Paths.get(resultFile), LinkOption.NOFOLLOW_LINKS)) {
      msg =  Files.readString(Paths.get(resultFile));
    }
    
    assertEquals("Testing test-rat-sources.sh failed: " + msg, 0, result.returnCode);
  }

  @Test
  public void testCheckSources() throws Exception {
    System.out.println("Start test-check-sources.sh test in Docker container (" + env + ") ...");
    String[] cmd = new String[]{"bash", "src/buildTest/scripts/test-check-sources.sh", "-r", resultFile};
    PbResult result = runCmd(cmd, env, false, false, false);
    
    String msg = "";
    
    if (Files.exists(Paths.get(resultFile), LinkOption.NOFOLLOW_LINKS)) {
      msg =  Files.readString(Paths.get(resultFile));
    }
    
    assertEquals("Testing test-check-sources.sh failed: " + msg, 0, result.returnCode);
  }
}
