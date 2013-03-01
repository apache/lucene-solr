package org.apache.lucene.facet.taxonomy.directory;

import org.apache.lucene.util.BytesRef;

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

/**
 * @lucene.experimental
 */
abstract class Consts {

  static final String FULL = "$full_path$";
  static final String FIELD_PAYLOADS = "$payloads$";
  static final String PAYLOAD_PARENT = "p";
  static final BytesRef PAYLOAD_PARENT_BYTES_REF = new BytesRef(PAYLOAD_PARENT);

  /**
   * Delimiter used for creating the full path of a category from the list of
   * its labels from root. It is forbidden for labels to contain this
   * character.
   * <P>
   * Originally, we used \uFFFE, officially a "unicode noncharacter" (invalid
   * unicode character) for this purpose. Recently, we switched to the
   * "private-use" character \uF749.  Even more recently, we
   * switched to \U001F (INFORMATION_SEPARATOR).
   */
  //static final char DEFAULT_DELIMITER = '\uFFFE';
  //static final char DEFAULT_DELIMITER = '\uF749';
  static final char DEFAULT_DELIMITER = '\u001F';
}
