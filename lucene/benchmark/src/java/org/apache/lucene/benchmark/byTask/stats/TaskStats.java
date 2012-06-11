package org.apache.lucene.benchmark.byTask.stats;

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

import org.apache.lucene.benchmark.byTask.tasks.PerfTask;

/**
 * Statistics for a task run. 
 * <br>The same task can run more than once, but, if that task records statistics, 
 * each run would create its own TaskStats.
 */
public class TaskStats implements Cloneable {

  /** task for which data was collected */
  private PerfTask task; 

  /** round in which task run started */
  private int round;

  /** task start time */
  private long start;
  
  /** task elapsed time.  elapsed >= 0 indicates run completion! */
  private long elapsed = -1;
  
  /** max tot mem during task */
  private long maxTotMem;
  
  /** max used mem during task */
  private long maxUsedMem;
  
  /** serial run number of this task run in the perf run */
  private int taskRunNum;
  
  /** number of other tasks that started to run while this task was still running */ 
  private int numParallelTasks;
  
  /** number of work items done by this task.
   * For indexing that can be number of docs added.
   * For warming that can be number of scanned items, etc. 
   * For repeating tasks, this is a sum over repetitions.
   */
  private int count;

  /** Number of similar tasks aggregated into this record.   
   * Used when summing up on few runs/instances of similar tasks.
   */
  private int numRuns = 1;
  
  /**
   * Create a run data for a task that is starting now.
   * To be called from Points.
   */
  TaskStats (PerfTask task, int taskRunNum, int round) {
    this.task = task;
    this.taskRunNum = taskRunNum;
    this.round = round;
    maxTotMem = Runtime.getRuntime().totalMemory();
    maxUsedMem = maxTotMem - Runtime.getRuntime().freeMemory();
    start = System.currentTimeMillis();
  }
  
  /**
   * mark the end of a task
   */
  void markEnd (int numParallelTasks, int count) {
    elapsed = System.currentTimeMillis() - start;
    long totMem = Runtime.getRuntime().totalMemory();
    if (totMem > maxTotMem) {
      maxTotMem = totMem;
    }
    long usedMem = totMem - Runtime.getRuntime().freeMemory();
    if (usedMem > maxUsedMem) {
      maxUsedMem = usedMem;
    }
    this.numParallelTasks = numParallelTasks;
    this.count = count;
  }
  
  private int[] countsByTime;
  private long countsByTimeStepMSec;

  public void setCountsByTime(int[] counts, long msecStep) {
    countsByTime = counts;
    countsByTimeStepMSec = msecStep;
  }

  public int[] getCountsByTime() {
    return countsByTime;
  }

  public long getCountsByTimeStepMSec() {
    return countsByTimeStepMSec;
  }

  /**
   * @return the taskRunNum.
   */
  public int getTaskRunNum() {
    return taskRunNum;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder res = new StringBuilder(task.getName());
    res.append(" ");
    res.append(count);
    res.append(" ");
    res.append(elapsed);
    return res.toString();
  }

  /**
   * @return Returns the count.
   */
  public int getCount() {
    return count;
  }

  /**
   * @return elapsed time.
   */
  public long getElapsed() {
    return elapsed;
  }

  /**
   * @return Returns the maxTotMem.
   */
  public long getMaxTotMem() {
    return maxTotMem;
  }

  /**
   * @return Returns the maxUsedMem.
   */
  public long getMaxUsedMem() {
    return maxUsedMem;
  }

  /**
   * @return Returns the numParallelTasks.
   */
  public int getNumParallelTasks() {
    return numParallelTasks;
  }

  /**
   * @return Returns the task.
   */
  public PerfTask getTask() {
    return task;
  }

  /**
   * @return Returns the numRuns.
   */
  public int getNumRuns() {
    return numRuns;
  }

  /**
   * Add data from another stat, for aggregation
   * @param stat2 the added stat data.
   */
  public void add(TaskStats stat2) {
    numRuns += stat2.getNumRuns();
    elapsed += stat2.getElapsed();
    maxTotMem += stat2.getMaxTotMem();
    maxUsedMem += stat2.getMaxUsedMem();
    count += stat2.getCount();
    if (round != stat2.round) {
      round = -1; // no meaning if aggregating tasks of different round. 
    }

    if (countsByTime != null && stat2.countsByTime != null) {
      if (countsByTimeStepMSec != stat2.countsByTimeStepMSec) {
        throw new IllegalStateException("different by-time msec step");
      }
      if (countsByTime.length != stat2.countsByTime.length) {
        throw new IllegalStateException("different by-time msec count");
      }
      for(int i=0;i<stat2.countsByTime.length;i++) {
        countsByTime[i] += stat2.countsByTime[i];
      }
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Object#clone()
   */
  @Override
  public TaskStats clone() throws CloneNotSupportedException {
    TaskStats c = (TaskStats) super.clone();
    if (c.countsByTime != null) {
      c.countsByTime = c.countsByTime.clone();
    }
    return c;
  }

  /**
   * @return the round number.
   */
  public int getRound() {
    return round;
  }
  
}
