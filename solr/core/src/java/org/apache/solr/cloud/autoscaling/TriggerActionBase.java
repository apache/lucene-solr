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
package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.util.Map;

/**
 * Base class for {@link TriggerAction} implementations.
 */
public abstract class TriggerActionBase implements TriggerAction {

  protected Map<String, String> initArgs;

  @Override
  public String getName() {
    if (initArgs != null) {
      return initArgs.get("name");
    } else {
      return getClass().getSimpleName();
    }
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void init(Map<String, String> args) {
    this.initArgs = args;
  }
}
