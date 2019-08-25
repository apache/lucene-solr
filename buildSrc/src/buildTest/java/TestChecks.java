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
    String[] cmd = new String[]{"bash", "-c", "docker exec --user ${UID} -t ${CONTAINER_NAME} bash -c \"rm /home/lucene/project/solr/core/src/test/org/no_license_test_file.java\""};
    runCmd(cmd, env, false, false);
    
    cmd = new String[]{"bash", "-c", "docker exec --user ${UID} -t ${CONTAINER_NAME} bash -c \"rm /home/lucene/project/lucene/core/src/java/org/no_license_test_file.xml\""};
    runCmd(cmd, env, false, false);
    
    
    cmd = new String[]{"bash", "-c", "docker exec --user ${UID} -t ${CONTAINER_NAME} bash -c \"rm /home/lucene/project/solr/contrib/clustering/src/java/org/tab_file.xml\""};
    runCmd(cmd, env, false, false);
  }
  
  @Test
  public void testRatSources() throws Exception {
    System.out.println("Start test-rat-sources.sh test in Docker container (" + env + ") ...");
    String[] cmd = new String[]{"bash", "test-build-wdocker/test-rat-sources.sh"};
    PbResult result = runCmd(cmd, env, false, false, false);
    assertEquals("Testing test-rat-sources.sh failed", 0, result.returnCode);
  }

  @Test
  public void testCheckSources() throws Exception {
    System.out.println("Start test-check-sources.sh test in Docker container (" + env + ") ...");
    String[] cmd = new String[]{"bash", "test-build-wdocker/test-check-sources.sh"};
    PbResult result = runCmd(cmd, env, false, false, false);
    assertEquals("Testing test-check-sources.sh failed", 0, result.returnCode);
  }
}
