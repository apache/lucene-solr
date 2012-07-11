// -*- c-basic-offset: 2 -*-
package org.apache.lucene.analysis.morfologik;

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

import java.util.List;

import org.apache.lucene.util.Attribute;

/** 
 * Morfologik dictionaries provide morphosyntactic annotations for
 * surface forms. For the exact format and description of these,
 * see the project's documentation (annotations vary by dictionary!).
 */
public interface MorphosyntacticTagsAttribute extends Attribute {
  /** 
   * Set the POS tag. The default value (no-value) is null.
   * 
   * @param tags A list of POS tags corresponding to current lemma.
   */
  public void setTags(List<StringBuilder> tags);

  /** 
   * Returns the POS tag of the term.
   */
  public List<StringBuilder> getTags();

  /** Clear to default value. */
  public void clear();
}
