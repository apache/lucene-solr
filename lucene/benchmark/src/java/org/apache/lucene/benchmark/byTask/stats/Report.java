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

/**
 * Textual report of current statistics.
 */
public class Report {

  private String text;
  private int size;
  private int outOf;
  private int reported;

  public Report (String text, int size, int reported, int outOf) {
    this.text = text;
    this.size = size;
    this.reported = reported;
    this.outOf = outOf;
  }

  /**
   * Returns total number of stats points when this report was created.
   */
  public int getOutOf() {
    return outOf;
  }

  /**
   * Returns number of lines in the report.
   */
  public int getSize() {
    return size;
  }

  /**
   * Returns the report text.
   */
  public String getText() {
    return text;
  }

  /**
   * Returns number of stats points represented in this report.
   */
  public int getReported() {
    return reported;
  }
}
