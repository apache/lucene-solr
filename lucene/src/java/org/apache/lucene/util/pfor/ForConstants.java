package org.apache.lucene.util.pfor;

/**
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

// nocommit: remove this whole class after refactoring
public class ForConstants {
  /** nocommit: we dont need to write these */
  public static final int FOR_COMPRESSION = 1; /** encode compression method in header */
  public static final int PFOR_COMPRESSION = 2; /** to encode compression method in header */
  
  /** Index of header in int buffer */
  public static final int HEADER_INDEX = 0;
  public static final int HEADER_SIZE = 1; // one integer in IntBuffer
  
  /** Start index in int buffer of array integers each compressed to numFrameBits. */
  public static final int COMPRESSED_INDEX = HEADER_INDEX + 1;
}
