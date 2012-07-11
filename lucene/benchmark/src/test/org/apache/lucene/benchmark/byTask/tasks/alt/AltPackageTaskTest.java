package org.apache.lucene.benchmark.byTask.tasks.alt;

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

import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.byTask.Benchmark;

/** Tests that tasks in alternate packages are found. */
public class AltPackageTaskTest extends BenchmarkTestCase {

  /** Benchmark should fail loading the algorithm when alt is not specified */
  public void testWithoutAlt() throws Exception {
    try {
      execBenchmark(altAlg(false));
      assertFalse("Should have failed to run the algorithm",true);
    } catch(Exception e) {
      // expected exception, do nothing
    }
  }

  /** Benchmark should be able to load the algorithm when alt is specified */
  public void testWithAlt() throws Exception {
    Benchmark bm = execBenchmark(altAlg(true));
    assertNotNull(bm);
    assertNotNull(bm.getRunData().getPoints());
  }
  
  private String[] altAlg(boolean allowAlt) {
    String altTask = "{ AltTest }";
    if (allowAlt) {
      return new String[] {
          "alt.tasks.packages = " +this.getClass().getPackage().getName(),
          altTask
      };
    }
    return new String[] {
        altTask
    };
  }
}
