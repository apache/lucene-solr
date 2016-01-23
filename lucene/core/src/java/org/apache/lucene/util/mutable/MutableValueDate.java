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
package org.apache.lucene.util.mutable;

import java.util.Date;

/**
 * {@link MutableValue} implementation of type {@link Date}.
 * @see MutableValueLong
 */
public class MutableValueDate extends MutableValueLong {
  @Override
  public Object toObject() {
    return exists ? new Date(value) : null;
  }

  @Override
  public MutableValue duplicate() {
    MutableValueDate v = new MutableValueDate();
    v.value = this.value;
    v.exists = this.exists;
    return v;
  }  
}
