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
package org.apache.solr.search.facet;

public abstract class StrAggValueSource extends AggValueSource {
  protected String arg;

  public StrAggValueSource(String name, String arg) {
    super(name);
    this.arg = arg;
  }

  public String getArg() {
    return arg;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    String otherArg = ((StrAggValueSource)o).arg;
    if (arg == otherArg) return true;
    return (arg != null && arg.equals(otherArg));
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() + (arg == null ? 0 : arg.hashCode());
  }

  @Override
  public String description() {
    return name() + "(" + arg + ")";
  }
}


