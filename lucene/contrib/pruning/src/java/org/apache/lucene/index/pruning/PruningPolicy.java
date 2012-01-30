package org.apache.lucene.index.pruning;

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
 * General Definitions for Index Pruning, such as operations to be performed on field data.
 */
public class PruningPolicy {
  /** Delete (some or all) postings for this field. */
  public static final int DEL_POSTINGS = 0x01;
  /** Delete (some or all) stored values for this field. */
  public static final int DEL_STORED = 0x02;
  /** Delete term frequency vectors for this field (whole vectors or individual terms). */
  public static final int DEL_VECTOR = 0x04;
  /** Delete (some or all) payloads in these fields. */
  public static final int DEL_PAYLOADS = 0x08;
  /** Delete all data for this field. */
  public static final int DEL_ALL = 0xff;
}
