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

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

class AssertingStage extends Stage {
  final ArcAttribute arcAtt;
  final TermAttribute termAtt;
  private int lastFrom;
  private int lastTo;

  private final Set<Integer> allToNodes = new HashSet<Integer>();
  private final Set<Integer> allFromNodes = new HashSet<Integer>();

  public AssertingStage(Stage prevStage) {
    super(prevStage);
    arcAtt = prevStage.get(ArcAttribute.class);
    termAtt = prevStage.get(TermAttribute.class);
  }

  @Override
  public void reset(Reader reader) {
    super.reset(reader);
    allToNodes.clear();
    allToNodes.add(0);
    allFromNodes.clear();
  }

  @Override
  public boolean next() throws IOException {
    if (prevStage.next()) {
      int from = arcAtt.from();
      int to = arcAtt.to();

      if (allToNodes.contains(from) && !allFromNodes.contains(from)) {
        // OK: from is a "frontier" node (only has arriving
        // tokens and no leaving tokens yet)
      } else if (nodes.getCanChange(from)) {
        // OK: this node was explicitly reserved as still
        // having changes
      } else {
        throw new IllegalStateException("node=" + from + " is frozen, but current token (" + termAtt + ") uses it as from node");
      }

      allFromNodes.add(from);
      allToNodes.add(to);
      return true;
    } else {
      return false;
    }
  }
}
