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

package org.apache.lucene.luke.app.controllers.dto.overview;

public class TermCount {
  private String field;
  private long count;
  private String ratio;

  public static TermCount of(String field, long count, double numTerms) {
    TermCount tc = new TermCount();
    tc.field = field;
    tc.count = count;
    tc.ratio = String.format("%.2f %%", count / numTerms * 100);
    return tc;
  }

  private TermCount() {
  }

  public String getField() {
    return field;
  }

  public long getCount() {
    return count;
  }

  public String getRatio() {
    return ratio;
  }
}
