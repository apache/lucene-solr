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

public final class BinaryField extends Field {
    
  public static final FieldType DEFAULT_TYPE = new FieldType();
  static {
    DEFAULT_TYPE.setStored(true);
    DEFAULT_TYPE.freeze();
  }
  
  public BinaryField(String name, byte[] value) {
    super(name, BinaryField.DEFAULT_TYPE, value);
    this.isBinary = true;
  }
    
  public boolean isNumeric() {
    return false;
  }  
}
