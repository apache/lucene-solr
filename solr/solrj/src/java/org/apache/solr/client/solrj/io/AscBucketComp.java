package org.apache.solr.client.solrj.io;

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

import java.io.Serializable;
import java.util.Comparator;

public class AscBucketComp implements Comparator<BucketMetrics>, Serializable {

  private int ord;

  public AscBucketComp(int ord) {
    this.ord = ord;
  }

  public int compare(BucketMetrics b1, BucketMetrics b2) {
    double d1 = b1.getMetrics()[ord].getValue();
    double d2 = b2.getMetrics()[ord].getValue();
    if(d1 > d2) {
      return 1;
    } else if(d1 < d2) {
      return -1;
    } else {
      return 0;
    }
  }
}