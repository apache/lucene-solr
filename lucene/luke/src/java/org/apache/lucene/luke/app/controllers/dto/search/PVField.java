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

package org.apache.lucene.luke.app.controllers.dto.search;

import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;

import static org.apache.lucene.luke.app.controllers.dto.search.PVField.Type.INT;

public class PVField {
  private String field;
  private ObjectProperty<Type> typeProperty = new SimpleObjectProperty<>(INT);

  public static PVField of(String field) {
    PVField pvField = new PVField();
    pvField.field = field;
    return pvField;
  }

  private PVField() {
  }

  public String getField() {
    return field;
  }

  public ObjectProperty<Type> getTypeProperty() {
    return typeProperty;
  }

  public Type getType() {
    return typeProperty.get();
  }

  public void setType(Type value) {
    typeProperty.set(value);
  }

  public enum Type {
    INT, LONG, FLOAT, DOUBLE
  }
}
