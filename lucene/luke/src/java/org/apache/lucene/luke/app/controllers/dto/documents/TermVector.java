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

package org.apache.lucene.luke.app.controllers.dto.documents;

import org.apache.lucene.luke.models.documents.TermVectorEntry;

import java.util.stream.Collectors;

public class TermVector {
  private String termText;
  private Long freq;
  private String positions;
  private String offsets;

  public static TermVector of(TermVectorEntry entry) {
    TermVector tv = new TermVector();
    tv.termText = entry.getTermText();
    tv.freq = entry.getFreq();
    tv.positions = String.join(",",
        entry.getPositions().stream()
            .map(pos -> Integer.toString(pos.getPosition()))
            .collect(Collectors.toList()));
    tv.offsets = String.join(",",
        entry.getPositions().stream()
            .filter(pos -> pos.getStartOffset().isPresent() && pos.getEndOffset().isPresent())
            .map(pos -> String.format("%d-%d", pos.getStartOffset().orElse(-1), pos.getEndOffset().orElse(-1)))
            .collect(Collectors.toList())
    );
    return tv;
  }

  private TermVector() {
  }

  public String getTermText() {
    return termText;
  }

  public Long getFreq() {
    return freq;
  }

  public String getPositions() {
    return positions;
  }

  public String getOffsets() {
    return offsets;
  }
}
