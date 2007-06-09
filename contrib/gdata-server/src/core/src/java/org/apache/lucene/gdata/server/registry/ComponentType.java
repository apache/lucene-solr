package org.apache.lucene.gdata.server.registry;

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

import org.apache.lucene.gdata.search.SearchComponent;
import org.apache.lucene.gdata.server.ServiceFactory;
import org.apache.lucene.gdata.server.authentication.AuthenticationController;
import org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory;
import org.apache.lucene.gdata.storage.StorageController;

/**
 * The enumeration {@link ComponentType} defines the GDATA-Server Components 
 * available via {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry#lookup(Class, ComponentType)} 
 * method.
 * @see org.apache.lucene.gdata.server.registry.Component
 * @see org.apache.lucene.gdata.server.registry.GDataServerRegistry 
 * @author Simon Willnauer
 *
 */
public enum ComponentType {
    /**
     * StorageController Type
     * 
     * @see StorageController
     */
    @SuperType(superType = StorageController.class)
    STORAGECONTROLLER,
    /**
     * RequestHandlerFactory Type
     * 
     * @see RequestHandlerFactory
     */
    @SuperType(superType = RequestHandlerFactory.class)
    REQUESTHANDLERFACTORY,
    /**
     * SearchComponent Type
     * @see SearchComponent
     */
    @SuperType(superType = SearchComponent.class)
    SEARCHCONTROLLER,
    /**
     * ServiceFactory Type
     * 
     * @see ServiceFactory
     */
    @SuperType(superType = ServiceFactory.class)
    SERVICEFACTORY,
    /**
     * Super type for AuthenticationController implementations
     * @see AuthenticationController
     */
    @SuperType(superType = AuthenticationController.class)
    AUTHENTICATIONCONTROLLER

}
