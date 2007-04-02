package org.apache.lucene.benchmark.byTask.tasks;

import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.stats.Report;
import org.apache.lucene.benchmark.byTask.stats.TaskStats;
import org.apache.lucene.benchmark.byTask.utils.Format;

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

/**
 * Report (abstract) task - all report tasks extend this task.
 */
public abstract class ReportTask extends PerfTask {

  public ReportTask(PerfRunData runData) {
    super(runData);
  }

  /* (non-Javadoc)
   * @see PerfTask#shouldNeverLogAtStart()
   */
  protected boolean shouldNeverLogAtStart() {
    return true;
  }

  /* (non-Javadoc)
   * @see PerfTask#shouldNotRecordStats()
   */
  protected boolean shouldNotRecordStats() {
    return true;
  }

  /*
   * From here start the code used to generate the reports. 
   * Subclasses would use this part to generate reports.
   */
  
  protected static final String newline = System.getProperty("line.separator");
  
  /**
   * Get a textual summary of the benchmark results, average from all test runs.
   */
  protected static final String OP =          "Operation  ";
  protected static final String ROUND =       " round";
  protected static final String RUNCNT =      "   runCnt";
  protected static final String RECCNT =      "   recsPerRun";
  protected static final String RECSEC =      "        rec/s";
  protected static final String ELAPSED =     "  elapsedSec";
  protected static final String USEDMEM =     "    avgUsedMem";
  protected static final String TOTMEM =      "    avgTotalMem";
  protected static final String COLS[] = {
      RUNCNT,
      RECCNT,
      RECSEC,
      ELAPSED,
      USEDMEM,
      TOTMEM
  };

  /**
   * Compute a title line for a report table
   * @param longestOp size of longest op name in the table
   * @return the table title line.
   */
  protected String tableTitle (String longestOp) {
    StringBuffer sb = new StringBuffer();
    sb.append(Format.format(OP,longestOp));
    sb.append(ROUND);
    sb.append(getRunData().getConfig().getColsNamesForValsByRound());
    for (int i = 0; i < COLS.length; i++) {
      sb.append(COLS[i]);
    }
    return sb.toString(); 
  }
  
  /**
   * find the longest op name out of completed tasks.  
   * @param taskStats completed tasks to be considered.
   * @return the longest op name out of completed tasks.
   */
  protected String longestOp(Iterator taskStats) {
    String longest = OP;
    while (taskStats.hasNext()) {
      TaskStats stat = (TaskStats) taskStats.next();
      if (stat.getElapsed()>=0) { // consider only tasks that ended
        String name = stat.getTask().getName();
        if (name.length() > longest.length()) {
          longest = name;
        }
      }
    }
    return longest;
  }
  
  /**
   * Compute a report line for the given task stat.
   * @param longestOp size of longest op name in the table.
   * @param stat task stat to be printed.
   * @return the report line.
   */
  protected String taskReportLine(String longestOp, TaskStats stat) {
    PerfTask task = stat.getTask();
    StringBuffer sb = new StringBuffer();
    sb.append(Format.format(task.getName(), longestOp));
    String round = (stat.getRound()>=0 ? ""+stat.getRound() : "-");
    sb.append(Format.formatPaddLeft(round, ROUND));
    sb.append(getRunData().getConfig().getColsValuesForValsByRound(stat.getRound()));
    sb.append(Format.format(stat.getNumRuns(), RUNCNT)); 
    sb.append(Format.format(stat.getCount() / stat.getNumRuns(), RECCNT));
    long elapsed = (stat.getElapsed()>0 ? stat.getElapsed() : 1); // assume at least 1ms
    sb.append(Format.format(1,(float) (stat.getCount() * 1000.0 / elapsed), RECSEC));
    sb.append(Format.format(2, (float) stat.getElapsed() / 1000, ELAPSED));
    sb.append(Format.format(0, (float) stat.getMaxUsedMem() / stat.getNumRuns(), USEDMEM)); 
    sb.append(Format.format(0, (float) stat.getMaxTotMem() / stat.getNumRuns(), TOTMEM));
    return sb.toString();
  }

  protected Report genPartialReport(int reported, LinkedHashMap partOfTasks, int totalSize) {
    String longetOp = longestOp(partOfTasks.values().iterator());
    boolean first = true;
    StringBuffer sb = new StringBuffer();
    sb.append(tableTitle(longetOp));
    sb.append(newline);
    int lineNum = 0;
    for (Iterator it = partOfTasks.values().iterator(); it.hasNext();) {
      TaskStats stat = (TaskStats) it.next();
      if (!first) {
        sb.append(newline);
      }
      first = false;
      String line = taskReportLine(longetOp,stat);
      lineNum++;
      if (partOfTasks.size()>2 && lineNum%2==0) {
        line = line.replaceAll("   "," - ");
      }
      sb.append(line);
    }
    String reptxt = (reported==0 ? "No Matching Entries Were Found!" : sb.toString());
    return new Report(reptxt,partOfTasks.size(),reported,totalSize);
  }



}
