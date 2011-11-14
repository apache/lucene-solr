package org.apache.lucene.util;

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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

/** @lucene.internal */
public abstract class InfoStream implements Closeable {
  // Used for printing messages
  private static final AtomicInteger MESSAGE_ID = new AtomicInteger();
  protected final int messageID = MESSAGE_ID.getAndIncrement();
  
  /** prints a message */
  public abstract void message(String component, String message);
  
  private static InfoStream defaultInfoStream;
  
  /** The default infoStream (possibly null) used
   * by a newly instantiated classes.
   * @see #setDefault */
  public static InfoStream getDefault() {
    return defaultInfoStream;
  }
  
  /** Sets the default infoStream (possibly null) used
   * by a newly instantiated classes.
   * @see #setDefault */
  public static void setDefault(InfoStream infoStream) {
    defaultInfoStream = infoStream;
  }
}
