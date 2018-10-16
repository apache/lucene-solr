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
package org.apache.lucene.store;

import java.io.IOException;

/**
 * This exception is thrown when the <code>write.lock</code>
 * is lost.  This
 * happens when a writer is unable to keep the lock updated
 * (cannot hold the lock) and another writer could successfully
 * open the index. In such cases it is realized that the lock
 * is not already owned and such an exception is thrown.
 * @see LockFactory#obtainLock(Directory, String)
 */
public class LockLostException extends IOException {
  public LockLostException(String message) {
    super(message);
  }

  public LockLostException(String message, Throwable cause) {
    super(message, cause);

  }

  public LockLostException(Throwable cause) {
    super(cause);
  }
}
