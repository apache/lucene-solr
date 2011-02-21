package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

abstract class DocFieldConsumer {
  /** Called when DocumentsWriter decides to create a new
   *  segment */
  abstract void flush(Map<DocFieldConsumerPerThread,Collection<DocFieldConsumerPerField>> threadsAndFields, SegmentWriteState state) throws IOException;

  /** Called when an aborting exception is hit */
  abstract void abort();

  /** Add a new thread */
  abstract DocFieldConsumerPerThread addThread(DocFieldProcessorPerThread docFieldProcessorPerThread) throws IOException;

  /** Called when DocumentsWriter is using too much RAM.
   *  The consumer should free RAM, if possible, returning
   *  true if any RAM was in fact freed. */
  abstract boolean freeRAM();
  }
