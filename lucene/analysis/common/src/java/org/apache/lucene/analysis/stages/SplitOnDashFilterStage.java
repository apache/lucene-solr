package org.apache.lucene.analysis.stages;

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

import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.Attribute;

import java.io.IOException;
import java.io.Reader;

/** Simple example of decompounder-as-filter, just dividing
 *  a word at its dashes and keeping the original. */
public class SplitOnDashFilterStage extends Stage {

  // We change the term:
  private final TermAttribute termAttIn;
  private final TermAttribute termAttOut;

  // We change the to/from:
  private final ArcAttribute arcAttIn;
  private final ArcAttribute arcAttOut;

  private String[] parts;
  private int nextPart;

  public SplitOnDashFilterStage(Stage prevStage) {
    super(prevStage);
    termAttIn = prevStage.get(TermAttribute.class);
    termAttOut = create(TermAttribute.class);
    arcAttIn = prevStage.get(ArcAttribute.class);
    arcAttOut = create(ArcAttribute.class);
  }

  @Override
  public void reset(Reader reader) {
    super.reset(reader);
    parts = null;
  }

  @Override
  public boolean next() throws IOException {
    if (parts != null) {

      termAttOut.set(parts[nextPart]);
      int from;
      if (nextPart == 0) {
        from = arcAttIn.from();
      } else {
        from = arcAttOut.to();
      }
      int to;

      if (nextPart == 1) {
        // Clear our reservation as we output current token:
        nodes.wontChange(arcAttIn.from());
      }

      if (nextPart == parts.length-1) {
        to = arcAttIn.to();
        parts = null;
      } else {
        to = nodes.newNode();
        nextPart++;
      }
      arcAttOut.set(from, to);

      return true;
    }

    if (prevStage.next()) {

      // nocommit copyTo?
      termAttOut.set(termAttIn.get());
      arcAttOut.set(arcAttIn.from(), arcAttIn.to());
      
      parts = termAttIn.toString().split("-");
      if (parts.length == 1) {
        parts = null;
      } else {
        // Reserve right to change this node:
        nodes.mayChange(arcAttIn.from());
        nextPart = 0;
      }

      // nocommit: what to do about offset...
      return true;
    } else {
      return false;
    }
  }
}
