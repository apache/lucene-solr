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

package yipeng.join;

public class FData {

  public FData(long fId, String fName, String fType) {
    this.fId = fId;
    this.fName = fName;
    this.fType = fType;
  }

  private long fId;
  private String fName;
  private String fType;

  public long getfId() {
    return fId;
  }

  public void setfId(long fId) {
    this.fId = fId;
  }

  public String getfName() {
    return fName;
  }

  public void setfName(String fName) {
    this.fName = fName;
  }

  public String getfType() {
    return fType;
  }

  public void setfType(String fType) {
    this.fType = fType;
  }
}
