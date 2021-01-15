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

import java.util.List;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.stats.Report;
import org.apache.lucene.benchmark.byTask.stats.TaskStats;

/**
 * Report all statistics with no aggregations. <br>
 * Other side effects: None.
 */
public class RepAllTask extends ReportTask {

  public RepAllTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public int doLogic() throws Exception {
    Report rp = reportAll(getRunData().getPoints().taskStats());

    System.out.println();
    System.out.println(
        "------------> Report All (" + rp.getSize() + " out of " + rp.getOutOf() + ")");
    System.out.println(rp.getText());
    System.out.println();
    return 0;
  }

  /**
   * Report detailed statistics as a string
   *
   * @return the report
   */
  protected Report reportAll(List<TaskStats> taskStats) {
    String longestOp = longestOp(taskStats);
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    sb.append(tableTitle(longestOp));
    sb.append(newline);
    int reported = 0;
    for (final TaskStats stat : taskStats) {
      if (stat.getElapsed() >= 0) { // consider only tasks that ended
        if (!first) {
          sb.append(newline);
        }
        first = false;
        String line = taskReportLine(longestOp, stat);
        reported++;
        if (taskStats.size() > 2 && reported % 2 == 0) {
          line = line.replaceAll("   ", " - ");
        }
        sb.append(line);
      }
    }
    String reptxt = (reported == 0 ? "No Matching Entries Were Found!" : sb.toString());
    return new Report(reptxt, reported, reported, taskStats.size());
  }
}
