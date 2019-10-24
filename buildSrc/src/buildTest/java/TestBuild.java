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

public class TestBuild extends BaseTestClass {
  
  public TestBuild() {
    super();
  }
  
  @Before
  public void setUp() throws Exception {
  }
  
  @After
  public void tearDown() throws Exception {

  }
  
  @Test
  public void testBuild() throws Exception {
    System.out.println("Start test-build.sh test in Docker container (" + env + ") ...");
    String[] cmd = new String[]{"bash", "src/buildTest/scripts/test-build.sh", "-r", resultFile};
    PbResult result = runCmd(cmd, env, false, false, false);
    assertEquals("Testing test-build.sh failed", 0, result.returnCode);
  }

}
