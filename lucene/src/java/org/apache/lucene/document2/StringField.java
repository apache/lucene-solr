package org.apache.lucene.document2;

/**
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

public final class StringField extends Field {

  public static final FieldType TYPE_UNSTORED = new FieldType();
  public static final FieldType TYPE_STORED = new FieldType();
  static {
    TYPE_UNSTORED.setIndexed(true);
    TYPE_UNSTORED.setOmitNorms(true);
    TYPE_UNSTORED.setOmitTermFreqAndPositions(true);
    TYPE_UNSTORED.freeze();

    TYPE_STORED.setIndexed(true);
    TYPE_STORED.setStored(true);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.setOmitTermFreqAndPositions(true);
    TYPE_STORED.freeze();
  }
  
  public StringField(String name, boolean internName, String value) {
    super(name, StringField.TYPE_UNSTORED, value);
  }
  
  public StringField(String name, String value) {
    this(name, true, value);
  }
  
  public String stringValue() {
    return (fieldsData == null) ? null : fieldsData.toString();
  }
  
  public boolean isNumeric() {
    return false;
  }  
}
