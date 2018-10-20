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
package org.apache.lucene.util;

/**
 * Sneaky: rethrowing checked exceptions as unchecked
 * ones. Eh, it is sometimes useful...
 *
 * <p>Pulled from <a href="http://www.javapuzzlers.com">Java Puzzlers</a>.</p>
 * @see <a href="http://www.amazon.com/Java-Puzzlers-Traps-Pitfalls-Corner/dp/032133678X">http://www.amazon.com/Java-Puzzlers-Traps-Pitfalls-Corner/dp/032133678X</a>
 */
public final class Rethrow {
  private Rethrow() {}

  /**
   * Rethrows <code>t</code> (identical object).
   */
  public static void rethrow(Throwable t) {
    Rethrow.<Error>rethrow0(t);
  }
  
  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void rethrow0(Throwable t) throws T {
    throw (T) t;
  }
}

