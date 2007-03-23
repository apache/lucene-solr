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

import junit.framework.TestCase;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.tasks.TaskSequence;
import org.apache.lucene.benchmark.byTask.utils.Algorithm;

import java.io.File;
import java.io.StringReader;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Test very simply that perf tasks are parses as expected.
 */
public class TestPerfTasksParse extends TestCase {

  private static final boolean DEBUG = false;
  static final String NEW_LINE = System.getProperty("line.separator");
  static final String INDENT = "  ";

  // properties in effect in all tests here
  static final String propPart = 
    INDENT+"directory=RAMDirectory" + NEW_LINE +
    INDENT+"print.props=false" + NEW_LINE
  ;

  /*
   * All known tasks. 
   * As new tasks are added, add them here.
   * It would be nice to do that automatically, unfortunately
   * Java does not provide a "get all classes in package" or
   * "get all sub-classes" functionality.  
   */
  static String singleTaskAlgs [];
  
  /* (non-Javadoc)
   * @see junit.framework.TestCase#setUp()
   */
  protected void setUp() throws Exception {
    super.setUp();
    if (singleTaskAlgs==null) {
      singleTaskAlgs = findTasks();
    }
  }

  // one time initialization
  static String [] findTasks () throws Exception {
    ArrayList tsks = new ArrayList();
    // init with tasks we know about
    tsks.add(  " AddDoc                   "  );
    tsks.add(  " AddDoc(1000.0)           "  );
    tsks.add(  " ClearStats               "  );
    tsks.add(  " CloseIndex               "  );
    tsks.add(  " CloseReader              "  );
    tsks.add(  " CreateIndex              "  );
    tsks.add(  " DeleteDoc                "  );
    tsks.add(  " DeleteDoc(500.0)         "  );
    tsks.add(  " NewRound                 "  );
    tsks.add(  " OpenIndex                "  );
    tsks.add(  " OpenReader               "  );
    tsks.add(  " Optimize                 "  );
    tsks.add(  " RepAll                   "  );
    tsks.add(  " RepSelectByPref prefix   "  );
    tsks.add(  " RepSumByNameRound        "  );
    tsks.add(  " RepSumByName             "  );
    tsks.add(  " RepSumByPrefRound prefix "  );
    tsks.add(  " RepSumByPref   prefix    "  );
    tsks.add(  " ResetInputs              "  );
    tsks.add(  " ResetSystemErase         "  );
    tsks.add(  " ResetSystemSoft          "  );
    tsks.add(  " Search                   "  );
    tsks.add(  " SearchTravRet            "  );
    tsks.add(  " SearchTravRet(100.0)     "  );
    tsks.add(  " SearchTrav               "  );
    tsks.add(  " SearchTrav(50.0)         "  );
    tsks.add(  " SetProp                  "  );
    tsks.add(  " SetProp(name,value)      "  );
    tsks.add(  " Warm                     "  );
    tsks.add(  "SearchTravRetLoadFieldSelector");
    tsks.add("SearchTravRetLoadFieldSelector(body,title)");
    
    // if tasks.dir property is defined, look for additional tasks.
    // this somewhat covers tasks that would be added in the future, in case
    // the list above is not updated to cover them.
    // some tasks would be tested more than once this way, but that's ok.
    String tasksDir = System.getProperty("tasks.dir");
    if (tasksDir !=null) {
      String pkgPrefix = PerfTask.class.getPackage().getName()+".";
      String taskNames[] = new File(tasksDir).list();
      for (int i = 0; i < taskNames.length; i++) {
        String name = taskNames[i].trim();
        if (!name.endsWith("Task.class"))
          continue; // Task class file only
        name = name.substring(0,name.length()-6);
        Class cls = Class.forName(pkgPrefix+name);
        if (Modifier.isAbstract(cls.getModifiers()) || Modifier.isInterface(cls.getModifiers())) 
          continue; // skip sbstract classes
        if (!PerfTask.class.isAssignableFrom(cls))
          continue; // not a task
        name = name.substring(0,name.length()-4);
        if (name.startsWith("Rep") && name.indexOf("Pref")>=0)
          name += " prefix";
        tsks.add(" "+name+" ");
      }
    }
    return (String[]) tsks.toArray(new String[0]);
  }
  
  
  /**
   * @param name test name
   */
  public TestPerfTasksParse(String name) {
    super(name);
  }

  /**
   * Test the parsing of very simple tasks, for all tasks
   */
  public void testAllTasksSimpleParse() {
    doTestAllTasksSimpleParse(false,false);
  }
  
  /**
   * Test the parsing of simple sequential sequences, for all tasks
   */
  public void testAllTasksSimpleParseSequntial() {
    doTestAllTasksSimpleParse(true,false);
  }

  /**
   * Test the parsing of simple parallel sequences, for all tasks
   */
  public void testAllTasksSimpleParseParallel() {
    doTestAllTasksSimpleParse(true,true);
  }
  
  // utility for simple parsing testing of all tasks.
  private void doTestAllTasksSimpleParse(boolean parOrSeq, boolean par) {
    for (int i = 0; i < singleTaskAlgs.length; i++) {
      String testedTask = singleTaskAlgs[i];
      if (parOrSeq) {
        if (par) {
          testedTask = "[ " + testedTask + " ] : 2";
        } else {
          testedTask = "{ " + testedTask + " } : 3";
        }
      }
      try {
        String algText = propPart+INDENT+testedTask;
        logTstParsing(algText);
        Benchmark benchmark = new Benchmark(new StringReader(algText));
        Algorithm alg = benchmark.getAlgorithm();
        ArrayList algTasks = alg.extractTasks();
        // must find a task with this name in the algorithm
        boolean foundName = false;
        boolean foundPar = false;
        String theTask = singleTaskAlgs[i].replaceAll(" +"," ").trim();
        for (Iterator iter = algTasks.iterator(); iter.hasNext();) {
          PerfTask task = (PerfTask) iter.next();
          foundName |= (task.toString().indexOf(theTask)>=0);
          foundPar |= (task instanceof TaskSequence && ((TaskSequence)task).isParallel());
        }
        assertTrue("Task "+testedTask+" was not found in "+alg.toString(),foundName);
        if (parOrSeq) {
          if (par) {
            assertTrue("Task "+testedTask+" was supposed to be parallel in "+alg.toString(),foundPar);
          } else {
            assertFalse("Task "+testedTask+" was not supposed to be parallel in "+alg.toString(),foundPar);
          }
        }
      } catch (Exception e) {
        System.out.flush();
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }

  /**
   * Test the repetiotion parsing for parallel tasks
   */
  public void testParseParallelTaskSequenceRepetition() throws Exception {
    String taskStr = "AddDoc";
    String parsedTasks = "[ "+taskStr+" ] : 1000";
    Benchmark benchmark = new Benchmark(new StringReader(propPart+parsedTasks));
    Algorithm alg = benchmark.getAlgorithm();
    ArrayList algTasks = alg.extractTasks();
    boolean foundAdd = false;
    for (Iterator iter = algTasks.iterator(); iter.hasNext();) {
       PerfTask task = (PerfTask) iter.next();
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

  /**
   * Test the repetiotion parsing for sequential  tasks
   */
  public void testParseTaskSequenceRepetition() throws Exception {
    String taskStr = "AddDoc";
    String parsedTasks = "{ "+taskStr+" } : 1000";
    Benchmark benchmark = new Benchmark(new StringReader(propPart+parsedTasks));
    Algorithm alg = benchmark.getAlgorithm();
    ArrayList algTasks = alg.extractTasks();
    boolean foundAdd = false;
    for (Iterator iter = algTasks.iterator(); iter.hasNext();) {
       PerfTask task = (PerfTask) iter.next();
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

  private void logTstParsing (String txt) {
    if (!DEBUG) 
      return;
    System.out.println("Test parsing of");
    System.out.println(txt);
  }

}
