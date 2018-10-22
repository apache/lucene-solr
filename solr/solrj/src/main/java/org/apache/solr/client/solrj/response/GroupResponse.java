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
package org.apache.solr.client.solrj.response;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Overall grouping result. Contains a list of {@link GroupCommand} instances that is the result of
 * one the following parameters:
 * <ul>
 *   <li>group.field
 *   <li>group.func
 *   <li>group.query
 * </ul>
 *
 * @since solr 3.4
 */
public class GroupResponse implements Serializable {

  private final List<GroupCommand> _values = new ArrayList<>();

  /**
   * Adds a grouping command to the response.
   *
   * @param command The grouping command to add
   */
  public void add(GroupCommand command) {
    _values.add(command);
  }

  /**
   * Returns all grouping commands.
   *
   * @return all grouping commands
   */
  public List<GroupCommand> getValues() {
    return _values;
  }
}