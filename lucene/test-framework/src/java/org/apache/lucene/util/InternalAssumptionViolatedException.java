package org.apache.lucene.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.hamcrest.Description;
import org.junit.internal.AssumptionViolatedException;

/**
 * We have our own "custom" assumption class because JUnit's {@link AssumptionViolatedException}
 * does not allow a cause exception to be set.
 * 
 * <p>We currently subclass and substitute JUnit's internal AVE.
 */
@SuppressWarnings("serial") 
final class InternalAssumptionViolatedException extends AssumptionViolatedException {
  private final String message;

  public InternalAssumptionViolatedException(String message) {
    this(message, null);
  }

  public InternalAssumptionViolatedException(String message, Throwable t) {
    super(t, /* no matcher. */ null);
    if (getCause() != t) {
      throw new Error("AssumptionViolationException not setting up getCause() properly? Panic.");
    }
    this.message = message;
  }

  @Override
  public String getMessage() {
    return super.getMessage();
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("failed assumption: " + message);
    if (getCause() != null) {
      description.appendText("(throwable: " + getCause().toString() + ")");
    }
  }
}