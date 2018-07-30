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

package org.apache.lucene.luke.app.controllers.dto.analysis;

import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;

public class Param {
  private BooleanProperty deleted = new SimpleBooleanProperty(false);
  private String name = "";
  private String value = "";

  public static Param newInstance() {
    return new Param();
  }

  public static Param of(String name, String value) {
    Param param = new Param();
    param.name = name;
    param.value = value;
    return param;
  }

  private Param() {
  }

  public Boolean isDeleted() {
    return deleted.get();
  }

  public void setDeleted(boolean val) {
    deleted.set(val);
  }

  public BooleanProperty getDeletedProperty() {
    return deleted;
  }


  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public boolean isValid() {
    return name != null && name.length() > 0;
  }
}
