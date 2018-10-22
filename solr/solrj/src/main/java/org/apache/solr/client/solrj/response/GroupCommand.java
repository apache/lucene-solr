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
 * This class represents the result of a group command.
 * This can be the result of the following parameter:
 * <ul>
 *   <li> group.field
 *   <li> group.func
 *   <li> group.query
 * </ul>
 *
 * An instance of this class contains:
 * <ul>
 *   <li> The name of this command. This can be the field, function or query grouped by.
 *   <li> The total number of documents that have matched.
 *   <li> The total number of groups that have matched.
 *   <li> The groups to be displayed. Depending on the start and rows parameter.
 * </ul>
 *
 * In case of <code>group.query</code> only one group is present and ngroups is always <code>null</code>.
 *
 * @since solr 3.4
 */
public class GroupCommand implements Serializable {

  private final String _name;
  private final List<Group> _values = new ArrayList<>();
  private final int _matches;
  private final Integer _ngroups;

  /**
   * Creates a GroupCommand instance
   *
   * @param name    The name of this command
   * @param matches The total number of documents found for this command
   */
  public GroupCommand(String name, int matches) {
    _name = name;
    _matches = matches;
    _ngroups = null;
  }

  /**
   * Creates a GroupCommand instance.
   *
   * @param name    The name of this command
   * @param matches The total number of documents found for this command
   * @param nGroups The total number of groups found for this command.
   */
  public GroupCommand(String name, int matches, int nGroups) {
    _name = name;
    _matches = matches;
    _ngroups = nGroups;
  }

  /**
   * Returns the name of this command. This can be the field, function or query grouped by.
   *
   * @return the name of this command
   */
  public String getName() {
    return _name;
  }

  /**
   * Adds a group to this command.
   *
   * @param group A group to be added
   */
  public void add(Group group) {
    _values.add(group);
  }

  /**
   * Returns the groups to be displayed.
   * The number of groups returned depend on the <code>start</code> and <code>rows</code> parameters.
   *
   * @return the groups to be displayed.
   */
  public List<Group> getValues() {
    return _values;
  }

  /**
   * Returns the total number of documents found for this command.
   *
   * @return the total number of documents found for this command.
   */
  public int getMatches() {
    return _matches;
  }

  /**
   * Returns the total number of groups found for this command.
   * Returns <code>null</code> if the <code>group.ngroups</code> parameter is unset or <code>false</code> or
   * if this is a group command query (parameter = <code>group.query</code>).
   *
   * @return the total number of groups found for this command.
   */
  public Integer getNGroups() {
    return _ngroups;
  }

}