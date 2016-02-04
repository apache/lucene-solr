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


import java.util.LinkedHashMap;
import java.util.List;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.stats.Report;
import org.apache.lucene.benchmark.byTask.stats.TaskStats;

/**
 * Report all statistics aggregated by name.
 * <br>Other side effects: None.
 */
public class RepSumByNameTask extends ReportTask {

  public RepSumByNameTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public int doLogic() throws Exception {
    Report rp = reportSumByName(getRunData().getPoints().taskStats());

    System.out.println();
    System.out.println("------------> Report Sum By (any) Name ("+
        rp.getSize()+" about "+rp.getReported()+" out of "+rp.getOutOf()+")");
    System.out.println(rp.getText());
    System.out.println();

    return 0;
  }

  /**
   * Report statistics as a string, aggregate for tasks named the same.
   * @return the report
   */
  protected Report reportSumByName(List<TaskStats> taskStats) {
    // aggregate by task name
    int reported = 0;
    LinkedHashMap<String,TaskStats> p2 = new LinkedHashMap<>();
    for (final TaskStats stat1: taskStats) {
      if (stat1.getElapsed()>=0) { // consider only tasks that ended
        reported++;
        String name = stat1.getTask().getName();
        TaskStats stat2 = p2.get(name);
        if (stat2 == null) {
          try {
            stat2 = stat1.clone();
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
    return genPartialReport(reported, p2, taskStats.size());
  }


}
