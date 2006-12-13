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

package org.apache.lucene.gdata.server.registry;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * The Scope annotation is used in
 * {@link org.apache.lucene.gdata.server.registry.ScopeVisitable} and
 * {@link org.apache.lucene.gdata.server.registry.ScopeVisitor} implementations
 * to indicate which scope should be visited.
 * 
 * @author Simon Willnauer
 * 
 */
@Target( { TYPE })
@Retention(value = RUNTIME)
public @interface Scope {
    /**
     * @return - the scope type the class was annotated with
     */
    ScopeType scope();

    /**
     * Defines a Scope for {@link Scope} annotations
     * 
     * @author Simon Willnauer
     * 
     */
    public static enum ScopeType {
        /**
         * Request scope
         */
        REQUEST,
        /**
         * Session scope
         */
        SESSION,
        /**
         * Context scope
         */
        CONTEXT

    }
}
