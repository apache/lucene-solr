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

/**
 * <p>
 * <code>ScopeVisitor</code> is used to implement the <code>Visitor</code>
 * pattern in GDATAServer. An object of this interface can be passed to a
 * <code>ScopeVistable</code> which will then call its methods. <br/>
 * {@link org.apache.lucene.gdata.server.registry.Component} Classes registered
 * in the {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry}
 * will be added to the Visitableimplementation automatically. Please refer to
 * the <i>Gang of Four </i> book of Design Patterns for more details on the
 * <code>Visitor</code> pattern.
 * </p>
 * <p>
 * A scope can be Session, Request or Context if one of the ScopeVisitors for
 * the desired scope is available by the registry.
 * </p>
 * <p>
 * This <a href="http://www.patterndepot.com/put/8/JavaPatterns.htm">site </a>
 * has further discussion on design patterns and links to the GOF book. This <a
 * href="http://www.patterndepot.com/put/8/visitor.pdf">link </a> describes the
 * Visitor pattern in detail.
 * </p>
 * 
 * @author Simon Willnauer
 * 
 */
public interface ScopeVisitor {
    /**
     * Visites the initialization of the scope
     */
    public abstract void visiteInitialize();

    /**
     * Visites the destory of the scope
     * 
     */
    public abstract void visiteDestroy();
}
