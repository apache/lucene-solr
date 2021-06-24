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
package org.apache.lucene.benchmark.byTask.tasks;


import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.benchmark.Constants;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException;
import org.apache.lucene.benchmark.byTask.stats.TaskStats;
import org.apache.lucene.util.ArrayUtil;

/**
 * Sequence of parallel or sequential tasks.
 */
public class TaskSequence extends PerfTask {
  public static int REPEAT_EXHAUST = -2; 
  private ArrayList<PerfTask> tasks;
  private int repetitions = 1;
  private boolean parallel;
  private TaskSequence parent;
  private boolean letChildReport = true;
  private int rate = 0;
  private boolean perMin = false; // rate, if set, is, by default, be sec.
  private String seqName;
  private boolean exhausted = false;
  private boolean resetExhausted = false;
  private PerfTask[] tasksArray;
  private boolean anyExhaustibleTasks;
  private boolean collapsable = false; // to not collapse external sequence named in alg.  
  
  private boolean fixedTime;                      // true if we run for fixed time
  private double runTimeSec;                      // how long to run for
  private final long logByTimeMsec;

  public TaskSequence (PerfRunData runData, String name, TaskSequence parent, boolean parallel) {
    super(runData);
    collapsable = (name == null);
    name = (name!=null ? name : (parallel ? "Par" : "Seq"));
    setName(name);
    setSequenceName();
    this.parent = parent;
    this.parallel = parallel;
    tasks = new ArrayList<>();
    logByTimeMsec = runData.getConfig().get("report.time.step.msec", 0);
  }

  @Override
  public void close() throws Exception {
    initTasksArray();
    for(int i=0;i<tasksArray.length;i++) {
      tasksArray[i].close();
    }
    getRunData().getDocMaker().close();
  }

  private void initTasksArray() {
    if (tasksArray == null) {
      final int numTasks = tasks.size();
      tasksArray = new PerfTask[numTasks];
      for(int k=0;k<numTasks;k++) {
        tasksArray[k] = tasks.get(k);
        anyExhaustibleTasks |= tasksArray[k] instanceof ResetInputsTask;
        anyExhaustibleTasks |= tasksArray[k] instanceof TaskSequence;
      }
    }
    if (!parallel && logByTimeMsec != 0 && !letChildReport) {
      countsByTime = new int[1];
    }
  }

  /**
   * @return Returns the parallel.
   */
  public boolean isParallel() {
    return parallel;
  }

  /**
   * @return Returns the repetitions.
   */
  public int getRepetitions() {
    return repetitions;
  }

  private int[] countsByTime;

  public void setRunTime(double sec) throws Exception {
    runTimeSec = sec;
    fixedTime = true;
  }

  /**
   * @param repetitions The repetitions to set.
   */
  public void setRepetitions(int repetitions) throws Exception {
    fixedTime = false;
    this.repetitions = repetitions;
    if (repetitions==REPEAT_EXHAUST) {
      if (isParallel()) {
        throw new Exception("REPEAT_EXHAUST is not allowed for parallel tasks");
      }
    }
    setSequenceName();
  }

  /**
   * @return Returns the parent.
   */
  public TaskSequence getParent() {
    return parent;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#doLogic()
   */
  @Override
  public int doLogic() throws Exception {
    exhausted = resetExhausted = false;
    return ( parallel ? doParallelTasks() : doSerialTasks());
  }

  private static class RunBackgroundTask extends Thread {
    private final PerfTask task;
    private final boolean letChildReport;
    private volatile int count;

    public RunBackgroundTask(PerfTask task, boolean letChildReport) {
      this.task = task;
      this.letChildReport = letChildReport;
    }

    public void stopNow() throws InterruptedException {
      task.stopNow();
    }

    public int getCount() {
      return count;
    }

    @Override
    public void run() {
      try {
        count = task.runAndMaybeStats(letChildReport);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private int doSerialTasks() throws Exception {
    if (rate > 0) {
      return doSerialTasksWithRate();
    }
    
    initTasksArray();
    int count = 0;

    final long runTime = (long) (runTimeSec*1000);
    List<RunBackgroundTask> bgTasks = null;

    final long t0 = System.currentTimeMillis();
    for (int k=0; fixedTime || (repetitions==REPEAT_EXHAUST && !exhausted) || k<repetitions; k++) {
      if (stopNow) {
        break;
      }
      for(int l=0;l<tasksArray.length;l++) {
        final PerfTask task = tasksArray[l];
        if (task.getRunInBackground()) {
          if (bgTasks == null) {
            bgTasks = new ArrayList<>();
          }
          RunBackgroundTask bgTask = new RunBackgroundTask(task, letChildReport);
          bgTask.setPriority(task.getBackgroundDeltaPriority() + Thread.currentThread().getPriority());
          bgTask.start();
          bgTasks.add(bgTask);
        } else {
          try {
            final int inc = task.runAndMaybeStats(letChildReport);
            count += inc;
            if (countsByTime != null) {
              final int slot = (int) ((System.currentTimeMillis()-t0)/logByTimeMsec);
              if (slot >= countsByTime.length) {
                countsByTime = ArrayUtil.grow(countsByTime, 1+slot);
              }
              countsByTime[slot] += inc;
            }
            if (anyExhaustibleTasks)
              updateExhausted(task);
          } catch (NoMoreDataException e) {
            exhausted = true;
          }
        }
      }
      if (fixedTime && System.currentTimeMillis()-t0 > runTime) {
        repetitions = k+1;
        break;
      }
    }

    if (bgTasks != null) {
      for(RunBackgroundTask bgTask : bgTasks) {
        bgTask.stopNow();
      }
      for(RunBackgroundTask bgTask : bgTasks) {
        bgTask.join();
        count += bgTask.getCount();
      }
    }

    if (countsByTime != null) {
      getRunData().getPoints().getCurrentStats().setCountsByTime(countsByTime, logByTimeMsec);
    }

    stopNow = false;

    return count;
  }

  private int doSerialTasksWithRate() throws Exception {
    initTasksArray();
    long delayStep = (perMin ? 60000 : 1000) /rate;
    long nextStartTime = System.currentTimeMillis();
    int count = 0;
    final long t0 = System.currentTimeMillis();
    for (int k=0; (repetitions==REPEAT_EXHAUST && !exhausted) || k<repetitions; k++) {
      if (stopNow) {
        break;
      }
      for (int l=0;l<tasksArray.length;l++) {
        final PerfTask task = tasksArray[l];
        while(!stopNow) {
          long waitMore = nextStartTime - System.currentTimeMillis();
          if (waitMore > 0) {
            // TODO: better to use condition to notify
            Thread.sleep(1);
          } else {
            break;
          }
        }
        if (stopNow) {
          break;
        }
        nextStartTime += delayStep; // this aims at avarage rate. 
        try {
          final int inc = task.runAndMaybeStats(letChildReport);
          count += inc;
          if (countsByTime != null) {
            final int slot = (int) ((System.currentTimeMillis()-t0)/logByTimeMsec);
            if (slot >= countsByTime.length) {
              countsByTime = ArrayUtil.grow(countsByTime, 1+slot);
            }
            countsByTime[slot] += inc;
          }

          if (anyExhaustibleTasks)
            updateExhausted(task);
        } catch (NoMoreDataException e) {
          exhausted = true;
        }
      }
    }
    stopNow = false;
    return count;
  }

  // update state regarding exhaustion.
  private void updateExhausted(PerfTask task) {
    if (task instanceof ResetInputsTask) {
      exhausted = false;
      resetExhausted = true;
    } else if (task instanceof TaskSequence) {
      TaskSequence t = (TaskSequence) task;
      if (t.resetExhausted) {
        exhausted = false;
        resetExhausted = true;
        t.resetExhausted = false;
      } else {
        exhausted |= t.exhausted;
      }
    }
  }

  private class ParallelTask extends Thread {

    public int count;
    public final PerfTask task;

    public ParallelTask(PerfTask task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        int n = task.runAndMaybeStats(letChildReport);
        if (anyExhaustibleTasks) {
          updateExhausted(task);
        }
        count += n;
      } catch (NoMoreDataException e) {
        exhausted = true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void stopNow() {
    super.stopNow();
    // Forwards top request to children
    if (runningParallelTasks != null) {
      for(ParallelTask t : runningParallelTasks) {
        if (t != null) {
          t.task.stopNow();
        }
      }
    }
  }

  ParallelTask[] runningParallelTasks;

  private int doParallelTasks() throws Exception {

    final TaskStats stats = getRunData().getPoints().getCurrentStats();

    initTasksArray();
    ParallelTask t[] = runningParallelTasks = new ParallelTask[repetitions * tasks.size()];
    // Get number of parallel threads from algo file and set it to use in ReuersContentSource.java's
    // docCountArrInit()
    this.getRunData().getConfig().setNumThreads(t.length);
    // prepare threads
    int index = 0;
    for (int k=0; k<repetitions; k++) {
      for (int i = 0; i < tasksArray.length; i++) {
        final PerfTask task = tasksArray[i].clone();
        t[index] = new ParallelTask(task);
        // Setting unique ThreadName with index value which is used in ReuersContentSource.java's
        // getNextDocData().Please make changes
        // in ReuersContentSource.java's getNextDocData() for
        // Integer.parseInt(Thread.currentThread().getName().substring(parallelTaskThreadSize + 1,
        // threadIndexSize))
        // before making any modifications here
        t[index].setName(Constants.PARALLEL_TASK_THREAD_NAME_PREFIX + "-" + index);
        index++;
      }
    }
    // run threads
    startThreads(t);

    if (stopNow) {
      for (ParallelTask task : t) {
        task.task.stopNow();
      }
    }

    // wait for all threads to complete
    int count = 0;
    for (int i = 0; i < t.length; i++) {
      t[i].join();
      count += t[i].count;
      if (t[i].task instanceof TaskSequence) {
        TaskSequence sub = (TaskSequence) t[i].task;
        if (sub.countsByTime != null) {
          if (countsByTime == null) {
            countsByTime = new int[sub.countsByTime.length];
          } else if (countsByTime.length < sub.countsByTime.length) {
            countsByTime = ArrayUtil.grow(countsByTime, sub.countsByTime.length);
          }
          for(int j=0;j<sub.countsByTime.length;j++) {
            countsByTime[j] += sub.countsByTime[j];
          }
        }
      }
    }

    if (countsByTime != null) {
      stats.setCountsByTime(countsByTime, logByTimeMsec);
    }

    // return total count
    return count;
  }

  // run threads
  private void startThreads(ParallelTask[] t) throws InterruptedException {
    if (rate > 0) {
      startlThreadsWithRate(t);
      return;
    }
    for (int i = 0; i < t.length; i++) {
      t[i].start();
    }
  }

  // run threads with rate
  private void startlThreadsWithRate(ParallelTask[] t) throws InterruptedException {
    long delayStep = (perMin ? 60000 : 1000) /rate;
    long nextStartTime = System.currentTimeMillis();
    for (int i = 0; i < t.length; i++) {
      long waitMore = nextStartTime - System.currentTimeMillis();
      if (waitMore > 0) {
        Thread.sleep(waitMore);
      }
      nextStartTime += delayStep; // this aims at average rate of starting threads. 
      t[i].start();
    }
  }

  public void addTask(PerfTask task) {
    tasks.add(task);
    task.setDepth(getDepth()+1);
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    String padd = getPadding();
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(parallel ? " [" : " {");
    sb.append(NEW_LINE);
    for (final PerfTask task : tasks) {
      sb.append(task.toString());
      sb.append(NEW_LINE);
    }
    sb.append(padd);
    sb.append(!letChildReport ? ">" : (parallel ? "]" : "}"));
    if (fixedTime) {
      sb.append(' ').append(NumberFormat.getNumberInstance(Locale.ROOT).format(runTimeSec)).append('s');
    } else if (repetitions>1) {
      sb.append(" * ").append(repetitions);
    } else if (repetitions==REPEAT_EXHAUST) {
      sb.append(" * EXHAUST");
    }
    if (rate>0) {
      sb.append(",  rate: ").append(rate).append('/').append(perMin ? "min" : "sec");
    }
    if (getRunInBackground()) {
      sb.append(" &");
      int x = getBackgroundDeltaPriority();
      if (x != 0) {
        sb.append(x);
      }
    }
    return sb.toString();
  }

  /**
   * Execute child tasks in a way that they do not report their time separately.
   */
  public void setNoChildReport() {
    letChildReport  = false;
    for (final PerfTask task : tasks) {
      if (task instanceof TaskSequence) {
        ((TaskSequence)task).setNoChildReport();
  }
    }
  }

  /**
   * Returns the rate per minute: how many operations should be performed in a minute.
   * If 0 this has no effect.
   * @return the rate per min: how many operations should be performed in a minute.
   */
  public int getRate() {
    return (perMin ? rate : 60*rate);
  }

  /**
   * @param rate The rate to set.
   */
  public void setRate(int rate, boolean perMin) {
    this.rate = rate;
    this.perMin = perMin;
    setSequenceName();
  }

  private void setSequenceName() {
    seqName = super.getName();
    if (repetitions==REPEAT_EXHAUST) {
      seqName += "_Exhaust";
    } else if (repetitions>1) {
      seqName += "_"+repetitions;
    }
    if (rate>0) {
      seqName += "_" + rate + (perMin?"/min":"/sec"); 
    }
    if (parallel && seqName.toLowerCase(Locale.ROOT).indexOf("par")<0) {
      seqName += "_Par";
    }
  }

  @Override
  public String getName() {
    return seqName; // override to include more info 
  }

  /**
   * @return Returns the tasks.
   */
  public ArrayList<PerfTask> getTasks() {
    return tasks;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#clone()
   */
  @Override
  protected TaskSequence clone() throws CloneNotSupportedException {
    TaskSequence res = (TaskSequence) super.clone();
    res.tasks = new ArrayList<>();
    for (int i = 0; i < tasks.size(); i++) {
      res.tasks.add(tasks.get(i).clone());
    }
    return res;
  }

  /**
   * Return true if can be collapsed in case it is outermost sequence
   */
  public boolean isCollapsable() {
    return collapsable;
  }
  
}
