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
package org.apache.lucene.benchmark.byTask.stats;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.utils.Config;

/** Test run data points collected as the test proceeds. */
public class Points {

  // stat points ordered by their start time.
  // for now we collect points as TaskStats objects.
  // later might optimize to collect only native data.
  private ArrayList<TaskStats> points = new ArrayList<>();

  private int nextTaskRunNum = 0;

  private TaskStats currentStats;

  /** Create a Points statistics object. */
  public Points(Config config) {}

  /**
   * Return the current task stats. the actual task stats are returned, so caller should not modify
   * this task stats.
   *
   * @return current {@link TaskStats}.
   */
  public List<TaskStats> taskStats() {
    return points;
  }

  /**
   * Mark that a task is starting. Create a task stats for it and store it as a point.
   *
   * @param task the starting task.
   * @return the new task stats created for the starting task.
   */
  public synchronized TaskStats markTaskStart(PerfTask task, int round) {
    TaskStats stats = new TaskStats(task, nextTaskRunNum(), round);
    this.currentStats = stats;
    points.add(stats);
    return stats;
  }

  public TaskStats getCurrentStats() {
    return currentStats;
  }

  // return next task num
  private synchronized int nextTaskRunNum() {
    return nextTaskRunNum++;
  }

  /** mark the end of a task */
  public synchronized void markTaskEnd(TaskStats stats, int count) {
    int numParallelTasks = nextTaskRunNum - 1 - stats.getTaskRunNum();
    // note: if the stats were cleared, might be that this stats object is
    // no longer in points, but this is just ok.
    stats.markEnd(numParallelTasks, count);
  }

  /** Clear all data, prepare for more tests. */
  public void clearData() {
    points.clear();
  }
}
