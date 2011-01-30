/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.lucene.util;
import java.io.File;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.types.Parameter;
import org.apache.tools.ant.types.selectors.BaseExtendSelector;

/** Divides filesets into equal groups */
public class LuceneJUnitDividingSelector extends BaseExtendSelector {
  private int counter;
  /** Number of total parts to split. */
  private int divisor;
  /** Current part to accept. */
  private int part;

  @Override
  public void setParameters(Parameter[] pParameters) {
    super.setParameters(pParameters);
    for (int j = 0; j < pParameters.length; j++) {
      Parameter p = pParameters[j];
      if ("divisor".equalsIgnoreCase(p.getName())) {
        divisor = Integer.parseInt(p.getValue());
      }
      else if ("part".equalsIgnoreCase(p.getName())) {
        part = Integer.parseInt(p.getValue());
      }
      else {
        throw new BuildException("unknown " + p.getName());
      }
    }
  }

  @Override
  public void verifySettings() {
    super.verifySettings();
    if (divisor <= 0 || part <= 0) {
      throw new BuildException("part or divisor not set");
    }
    if (part > divisor) {
      throw new BuildException("part must be <= divisor");
    }
  }

  @Override
  public boolean isSelected(File dir, String name, File path) {
    counter = counter % divisor + 1;
    return counter == part;
  }
}
