package org.apache.lucene.util;

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
 * Sneaky: rethrowing checked exceptions as unchecked
 * ones. Eh, it is sometimes useful...
 *
 * <p>Pulled from <a href="http://www.javapuzzlers.com">Java Puzzlers</a>.</p>
 * @see "http://www.amazon.com/Java-Puzzlers-Traps-Pitfalls-Corner/dp/032133678X"
 */
@SuppressWarnings({"unchecked","rawtypes"})
public final class Rethrow {
  /**
   * Classy puzzler to rethrow any checked exception as an unchecked one.
   */
  private static class Rethrower<T extends Throwable> {
    private void rethrow(Throwable t) throws T {
      throw (T) t;
    }
  }
  
  /**
   * Rethrows <code>t</code> (identical object).
   */
  public static void rethrow(Throwable t) {
    new Rethrower<Error>().rethrow(t);
  }
}

