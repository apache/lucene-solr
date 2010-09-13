/**
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

package org.apache.lucene.benchmark.byTask;

import java.io.StringReader;
import java.util.ArrayList;

import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.tasks.TaskSequence;
import org.apache.lucene.benchmark.byTask.utils.Algorithm;
import org.apache.lucene.util.LuceneTestCase;

/** Test very simply that perf tasks are parses as expected. */
public class TestPerfTasksParse extends LuceneTestCase {

  static final String NEW_LINE = System.getProperty("line.separator");
  static final String INDENT = "  ";

  // properties in effect in all tests here
  static final String propPart = 
    INDENT + "directory=RAMDirectory" + NEW_LINE +
    INDENT + "print.props=false" + NEW_LINE
  ;

  /** Test the repetiotion parsing for parallel tasks */
  public void testParseParallelTaskSequenceRepetition() throws Exception {
    String taskStr = "AddDoc";
    String parsedTasks = "[ "+taskStr+" ] : 1000";
    Benchmark benchmark = new Benchmark(new StringReader(propPart+parsedTasks));
    Algorithm alg = benchmark.getAlgorithm();
    ArrayList<PerfTask> algTasks = alg.extractTasks();
    boolean foundAdd = false;
    for (final PerfTask task : algTasks) {
       if (task.toString().indexOf(taskStr)>=0) {
          foundAdd = true;
       }
       if (task instanceof TaskSequence) {
         assertEquals("repetions should be 1000 for "+parsedTasks, 1000, ((TaskSequence) task).getRepetitions());
         assertTrue("sequence for "+parsedTasks+" should be parallel!", ((TaskSequence) task).isParallel());
       }
       assertTrue("Task "+taskStr+" was not found in "+alg.toString(),foundAdd);
    }
  }

  /** Test the repetiotion parsing for sequential  tasks */
  public void testParseTaskSequenceRepetition() throws Exception {
    String taskStr = "AddDoc";
    String parsedTasks = "{ "+taskStr+" } : 1000";
    Benchmark benchmark = new Benchmark(new StringReader(propPart+parsedTasks));
    Algorithm alg = benchmark.getAlgorithm();
    ArrayList<PerfTask> algTasks = alg.extractTasks();
    boolean foundAdd = false;
    for (final PerfTask task : algTasks) {
       if (task.toString().indexOf(taskStr)>=0) {
          foundAdd = true;
       }
       if (task instanceof TaskSequence) {
         assertEquals("repetions should be 1000 for "+parsedTasks, 1000, ((TaskSequence) task).getRepetitions());
         assertFalse("sequence for "+parsedTasks+" should be sequential!", ((TaskSequence) task).isParallel());
       }
       assertTrue("Task "+taskStr+" was not found in "+alg.toString(),foundAdd);
    }
  }

}
