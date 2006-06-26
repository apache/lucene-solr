/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.gdata.servlet.handler;

import org.apache.lucene.gdata.server.registry.ServerComponent;

/**
 * Abstract Superclass for RequestHandlerFactories
 * @author Simon Willnauer
 * 
 */
public abstract class RequestHandlerFactory implements ServerComponent {
    



    /**
     * public constructor to enable loading via the registry
     * @see org.apache.lucene.gdata.server.registry.Component
     * @see org.apache.lucene.gdata.server.registry.GDataServerRegistry
     */
    public RequestHandlerFactory() {
        super();

    }


    /**
     * Creates a EntryUpdateHandler which processes a GDATA UPDATE request.
     * @return - a RequestHandlerInstance
     */
    public abstract GDataRequestHandler getEntryUpdateHandler();

    /**
     * Creates a EntryDeleteHandler which processes a GDATA DELETE request.
     * @return - a RequestHandlerInstance
     */
    public abstract GDataRequestHandler getEntryDeleteHandler();

    /**
     * Creates a FeedQueryHandler which processes a GDATA Query / Get request.
     * @return - a RequestHandlerInstance
     */
    public abstract GDataRequestHandler getFeedQueryHandler();

    /**
     * Creates a EntryInsertHandler which processes a GDATA Insert request.
     * @return - a RequestHandlerInstance
     */
    public abstract GDataRequestHandler getEntryInsertHandler();
    /**
     * Creates a InsertAccountHandler which processes a Account Insert request.
     * @return - a RequestHandlerInstance
     */
    public abstract GDataRequestHandler getInsertAccountHandler();
    /**
     * Creates a DeleteAccountHandler which processes a Account Delete request.
     * @return - a RequestHandlerInstance
     */
    public abstract GDataRequestHandler getDeleteAccountHandler();
    /**
     * Creates a UpdateAccountHandler which processes a Account Update request.
     * @return - a RequestHandlerInstance
     */
    public abstract GDataRequestHandler getUpdateAccountHandler();
    /**
     * Creates a InsertFeedHandler which processes a Feed Insert request.
     * @return - a RequestHandlerInstance
     */
    public abstract GDataRequestHandler getInsertFeedHandler();
    /**
     * Creates a UpdateFeedHandler which processes a Feed Insert request.
     * @return - a RequestHandlerInstance
     */
    public abstract GDataRequestHandler getUpdateFeedHandler();
    /**
     * Creates a DeleteFeedHandler which processes a Feed Insert request.
     * @return - a RequestHandlerInstance
     */
    public abstract GDataRequestHandler getDeleteFeedHandler();
    

    
}
