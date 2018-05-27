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

package org.apache.lucene.luke.app.util;

import org.apache.lucene.luke.app.controllers.LukeController;
import org.apache.lucene.luke.models.LukeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class ExceptionHandler {

  private static final Logger logger = LoggerFactory.getLogger(ExceptionHandler.class);

  public static class ExceptionWrapper extends RuntimeException {
    ExceptionWrapper(Throwable cause) {
      super(cause);
    }
  }

  @FunctionalInterface
  public interface ConsumerWithException<T, E extends Exception> {
    void accept(T t) throws E;
  }

  @FunctionalInterface
  public interface RunnableWithException<E extends Exception> {
    void run() throws E;
  }

  public static <T, E extends Exception> Consumer<T> consumerWrapper(ConsumerWithException<T, E> consumer) {
    return t -> {
      try {
        consumer.accept(t);
      } catch (Exception e) {
        throw wrap(e);
      }
    };
  }

  public static <E extends Exception> Runnable runnableWrapper(RunnableWithException<E> runnable) {
    {
      try {
        runnable.run();
      } catch (Exception e) {
        throw wrap(e);
      }
      return null;
    }
  }

  public static void handle(Throwable t, LukeController lukeController) {
    if (t instanceof ExceptionWrapper) {
      Throwable cause = unwrap((ExceptionWrapper) t);
      if (cause instanceof LukeException) {
        lukeController.showStatusMessage(cause.getMessage());
      } else {
        logger.error(cause.getMessage(), cause);
        lukeController.showUnknownErrorMessage();
      }
    } else {
      logger.error(t.getMessage(), t);
      lukeController.showUnknownErrorMessage();
    }
  }

  private static ExceptionWrapper wrap(Exception cause) {
    return new ExceptionWrapper(cause);
  }

  private static Throwable unwrap(ExceptionWrapper wrapper) {
    return wrapper.getCause();
  }
}
