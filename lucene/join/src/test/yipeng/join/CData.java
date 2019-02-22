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

public class CData {
  private long cId;
  private long fId;
  private String cName;
  private double price;

  public CData(long cId, long  fId , String cName, double price) {
    this.cId = cId;
    this.cName = cName;
    this.price = price;
    this.fId = fId;
  }

  public long getcId() {
    return cId;
  }

  public void setcId(long cId) {
    this.cId = cId;
  }

  public String getcName() {
    return cName;
  }

  public void setcName(String cName) {
    this.cName = cName;
  }

  public double getPrice() {
    return price;
  }

  public void setPrice(double price) {
    this.price = price;
  }

  public long getfId() {
    return fId;
  }

  public void setfId(long fId) {
    this.fId = fId;
  }
}
