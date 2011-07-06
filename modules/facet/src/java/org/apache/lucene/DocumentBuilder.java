package org.apache.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

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

/**
 * An interface which standardizes the process of building an indexable
 * {@link Document}.
 * <p>
 * The idea is that implementations implement {@link #build(Document doc)},
 * which adds to the given Document whatever {@link Field}s it wants to add. A
 * DocumentBuilder is also allowed to inspect or change existing Fields in the
 * Document, if it wishes to.
 * <p>
 * Implementations should normally have a constructor with parameters which
 * determine what {@link #build(Document)} will add to doc.<br>
 * To allow reuse of the DocumentBuilder object, implementations are also
 * encouraged to have a setter method, which remembers its parameters just like
 * the constructor. This setter method cannot be described in this interface,
 * because it will take different parameters in each implementation.
 * <p>
 * The interface defines a builder pattern, which allows applications to invoke
 * several document builders in the following way:
 * 
 * <pre>
 * builder1.build(builder2.build(builder3.build(new Document())));
 * </pre>
 * 
 * @lucene.experimental
 */
public interface DocumentBuilder {
 
  /** An exception thrown from {@link DocumentBuilder}'s build(). */
  public static class DocumentBuilderException extends Exception {

    public DocumentBuilderException() {
      super();
    }

    public DocumentBuilderException(String message) {
      super(message);
    }

    public DocumentBuilderException(String message, Throwable cause) {
      super(message, cause);
    }

    public DocumentBuilderException(Throwable cause) {
      super(cause);
    }

  }

  /**
   * Adds to the given document whatever {@link Field}s the implementation needs
   * to add. Return the docunment instance to allow for chaining calls.
   */
  public Document build(Document doc) throws DocumentBuilderException;
  
}
