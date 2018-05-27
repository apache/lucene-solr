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

import org.apache.lucene.luke.util.BytesRefUtils;

public class TermPosting {

  private int position = -1;
  private String offset = "";
  private String payload = "";

  public static TermPosting of(org.apache.lucene.luke.models.documents.TermPosting p) {
    TermPosting posting = new TermPosting();
    posting.position = p.getPosition();
    if (p.getStartOffset() >= 0 && p.getEndOffset() >= 0) {
      posting.offset = String.format("%d-%d", p.getStartOffset(), p.getEndOffset());
    }
    if (p.getPayload() != null) {
      posting.payload = BytesRefUtils.decode(p.getPayload());
    }
    return posting;
  }

  private TermPosting() {
  }

  public int getPosition() {
    return position;
  }

  public String getOffset() {
    return offset;
  }

  public String getPayload() {
    return payload;
  }
}
