package org.apache.lucene.benchmark.byTask.stats;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.Format;


/**
 * Test run data points collected as the test proceeds.
 */
public class Points {

  private Config config;
  
  private static final String newline = System.getProperty("line.separator");
  
  // stat points ordered by their start time. 
  // for now we collect points as TaskStats objects.
  // later might optimize to collect only native data.
  private ArrayList points = new ArrayList();

  private int nextTaskRunNum = 0;

  /**
   * Get a textual summary of the benchmark results, average from all test runs.
   */
  static final String OP =          "Operation  ";
  static final String ROUND =       " round";
  static final String RUNCNT =      "   runCnt";
  static final String RECCNT =      "   recsPerRun";
  static final String RECSEC =      "        rec/s";
  static final String ELAPSED =     "  elapsedSec";
  static final String USEDMEM =     "    avgUsedMem";
  static final String TOTMEM =      "    avgTotalMem";
  static final String COLS[] = {
      RUNCNT,
      RECCNT,
      RECSEC,
      ELAPSED,
      USEDMEM,
      TOTMEM
  };

  /**
   * Create a Points statistics object. 
   */
  public Points (Config config) {
    this.config = config;
  }

  private String tableTitle (String longestOp) {
    StringBuffer sb = new StringBuffer();
    sb.append(Format.format(OP,longestOp));
    sb.append(ROUND);
    sb.append(config.getColsNamesForValsByRound());
    for (int i = 0; i < COLS.length; i++) {
      sb.append(COLS[i]);
    }
    return sb.toString(); 
  }
  
  /**
   * Report detailed statistics as a string
   * @return the report
   */
  public Report reportAll() {
    String longestOp = longestOp(points);
    boolean first = true;
    StringBuffer sb = new StringBuffer();
    sb.append(tableTitle(longestOp));
    sb.append(newline);
    int reported = 0;
    for (Iterator it = points.iterator(); it.hasNext();) {
      TaskStats stat = (TaskStats) it.next();
      if (stat.getElapsed()>=0) { // consider only tasks that ended
        if (!first) {
          sb.append(newline);
        }
        first = false;
        String line = taskReportLine(longestOp, stat);
        reported++;
        if (points.size()>2&& reported%2==0) {
          line = line.replaceAll("   "," - ");
        }
        sb.append(line);
      }
    }
    String reptxt = (reported==0 ? "No Matching Entries Were Found!" : sb.toString());
    return new Report(reptxt,reported,reported,points.size());
  }

  /**
   * Report statistics as a string, aggregate for tasks named the same.
   * @return the report
   */
  public Report reportSumByName() {
    // aggregate by task name
    int reported = 0;
    LinkedHashMap p2 = new LinkedHashMap();
    for (Iterator it = points.iterator(); it.hasNext();) {
      TaskStats stat1 = (TaskStats) it.next();
      if (stat1.getElapsed()>=0) { // consider only tasks that ended
        reported++;
        String name = stat1.getTask().getName();
        TaskStats stat2 = (TaskStats) p2.get(name);
        if (stat2 == null) {
          try {
            stat2 = (TaskStats) stat1.clone();
          } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
          }
          p2.put(name,stat2);
        } else {
          stat2.add(stat1);
        }
      }
    }
    // now generate report from secondary list p2    
    return genReportFromList(reported, p2);
  }

  /**
   * Report statistics as a string, aggregate for tasks named the same, and from the same round.
   * @return the report
   */
  public Report reportSumByNameRound() {
    // aggregate by task name and round
    LinkedHashMap p2 = new LinkedHashMap();
    int reported = 0;
    for (Iterator it = points.iterator(); it.hasNext();) {
      TaskStats stat1 = (TaskStats) it.next();
      if (stat1.getElapsed()>=0) { // consider only tasks that ended
        reported++;
        String name = stat1.getTask().getName();
        String rname = stat1.getRound()+"."+name; // group by round
        TaskStats stat2 = (TaskStats) p2.get(rname);
        if (stat2 == null) {
          try {
            stat2 = (TaskStats) stat1.clone();
          } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
          }
          p2.put(rname,stat2);
        } else {
          stat2.add(stat1);
        }
      }
    }
    // now generate report from secondary list p2    
    return genReportFromList(reported, p2);
  }
  
  private String longestOp(Collection c) {
    String longest = OP;
    for (Iterator it = c.iterator(); it.hasNext();) {
      TaskStats stat = (TaskStats) it.next();
      if (stat.getElapsed()>=0) { // consider only tasks that ended
        String name = stat.getTask().getName();
        if (name.length() > longest.length()) {
          longest = name;
        }
      }
    }
    return longest;
  }

  private String taskReportLine(String longestOp, TaskStats stat) {
    PerfTask task = stat.getTask();
    StringBuffer sb = new StringBuffer();
    sb.append(Format.format(task.getName(), longestOp));
    String round = (stat.getRound()>=0 ? ""+stat.getRound() : "-");
    sb.append(Format.formatPaddLeft(round, ROUND));
    sb.append(config.getColsValuesForValsByRound(stat.getRound()));
    sb.append(Format.format(stat.getNumRuns(), RUNCNT)); 
    sb.append(Format.format(stat.getCount() / stat.getNumRuns(), RECCNT));
    long elapsed = (stat.getElapsed()>0 ? stat.getElapsed() : 1); // assume at least 1ms
    sb.append(Format.format(1,(float) (stat.getCount() * 1000.0 / elapsed), RECSEC));
    sb.append(Format.format(2, (float) stat.getElapsed() / 1000, ELAPSED));
    sb.append(Format.format(0, (float) stat.getMaxUsedMem() / stat.getNumRuns(), USEDMEM)); 
    sb.append(Format.format(0, (float) stat.getMaxTotMem() / stat.getNumRuns(), TOTMEM));
    return sb.toString();
  }

  public Report reportSumByPrefix(String prefix) {
    // aggregate by task name
    int reported = 0;
    LinkedHashMap p2 = new LinkedHashMap();
    for (Iterator it = points.iterator(); it.hasNext();) {
      TaskStats stat1 = (TaskStats) it.next();
      if (stat1.getElapsed()>=0 && stat1.getTask().getName().startsWith(prefix)) { // only ended tasks with proper name
        reported++;
        String name = stat1.getTask().getName();
        TaskStats stat2 = (TaskStats) p2.get(name);
        if (stat2 == null) {
          try {
            stat2 = (TaskStats) stat1.clone();
          } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
          }
          p2.put(name,stat2);
        } else {
          stat2.add(stat1);
        }
      }
    }
    // now generate report from secondary list p2    
    return genReportFromList(reported, p2);
  }
  
  public Report reportSumByPrefixRound(String prefix) {
    // aggregate by task name and by round
    int reported = 0;
    LinkedHashMap p2 = new LinkedHashMap();
    for (Iterator it = points.iterator(); it.hasNext();) {
      TaskStats stat1 = (TaskStats) it.next();
      if (stat1.getElapsed()>=0 && stat1.getTask().getName().startsWith(prefix)) { // only ended tasks with proper name
        reported++;
        String name = stat1.getTask().getName();
        String rname = stat1.getRound()+"."+name; // group by round
        TaskStats stat2 = (TaskStats) p2.get(rname);
        if (stat2 == null) {
          try {
            stat2 = (TaskStats) stat1.clone();
          } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
          }
          p2.put(rname,stat2);
        } else {
          stat2.add(stat1);
        }
      }
    }
    // now generate report from secondary list p2    
    return genReportFromList(reported, p2);
  }

  private Report genReportFromList(int reported, LinkedHashMap p2) {
    String longetOp = longestOp(p2.values());
    boolean first = true;
    StringBuffer sb = new StringBuffer();
    sb.append(tableTitle(longetOp));
    sb.append(newline);
    int lineNum = 0;
    for (Iterator it = p2.values().iterator(); it.hasNext();) {
      TaskStats stat = (TaskStats) it.next();
      if (!first) {
        sb.append(newline);
      }
      first = false;
      String line = taskReportLine(longetOp,stat);
      lineNum++;
      if (p2.size()>2&& lineNum%2==0) {
        line = line.replaceAll("   "," - ");
      }
      sb.append(line);
    }
    String reptxt = (reported==0 ? "No Matching Entries Were Found!" : sb.toString());
    return new Report(reptxt,p2.size(),reported,points.size());
  }

  public Report reportSelectByPrefix(String prefix) {
    String longestOp = longestOp(points);
    boolean first = true;
    StringBuffer sb = new StringBuffer();
    sb.append(tableTitle(longestOp));
    sb.append(newline);
    int reported = 0;
    for (Iterator it = points.iterator(); it.hasNext();) {
      TaskStats stat = (TaskStats) it.next();
      if (stat.getElapsed()>=0 && stat.getTask().getName().startsWith(prefix)) { // only ended tasks with proper name
        reported++;
        if (!first) {
          sb.append(newline);
        }
        first = false;
        String line = taskReportLine(longestOp,stat);
        if (points.size()>2&& reported%2==0) {
          line = line.replaceAll("   "," - ");
        }
        sb.append(line);
      }
    }
    String reptxt = (reported==0 ? "No Matching Entries Were Found!" : sb.toString());
    return new Report(reptxt,reported,reported, points.size());
  }

  /**
   * Mark that a task is starting. 
   * Create a task stats for it and store it as a point.
   * @param task the starting task.
   * @return the new task stats created for the starting task.
   */
  public synchronized TaskStats markTaskStart (PerfTask task, int round) {
    TaskStats stats = new TaskStats(task, nextTaskRunNum(), round);
    points.add(stats);
    return stats;
  }
  
  // return next task num
  private synchronized int nextTaskRunNum() {
    return nextTaskRunNum++;
  }
  
  /**
   * mark the end of a task
   */
  public synchronized void markTaskEnd (TaskStats stats, int count) {
    int numParallelTasks = nextTaskRunNum - 1 - stats.getTaskRunNum();
    // note: if the stats were cleared, might be that this stats object is 
    // no longer in points, but this is just ok.
    stats.markEnd(numParallelTasks, count);
  }

  /**
   * Clear all data, prepare for more tests.
   */
  public void clearData() {
    points.clear();
  }

}
