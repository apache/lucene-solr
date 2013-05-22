package org.apache.lucene.analysis.ko.morph;

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

public class Status {

  private int josaMaxStart = 0;
  
  private int eomiMaxStart = 0;
  
  private int maxStart = 0;
  
  public void apply(int num) {
    if(maxStart<num) maxStart = num;
  }
  
  public int getMaxStart() {
    return maxStart;
  }

  public void setMaxStart(int maxStart) {
    this.maxStart = maxStart;
  }

  public int getJosaMaxStart() {
    return josaMaxStart;
  }

  public void setJosaMaxStart(int josaMaxStart) {
    this.josaMaxStart = josaMaxStart;
  }


  public int getEomiMaxStart() {
    return eomiMaxStart;
  }

  public void setEomiMaxStart(int eomiMaxStart) {
    this.eomiMaxStart = eomiMaxStart;
  }
}
