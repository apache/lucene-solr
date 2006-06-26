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

import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;

/**
 * Default implementation for RequestHandlerFactory Builds the
 * {@link org.apache.lucene.gdata.servlet.handler.GDataRequestHandler}
 * instances.
 * This class should not be access directy. The class will be registered in the {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry}.
 * Use {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry#lookup(Class, ComponentType)}
 * 
 * @author Simon Willnauer
 * 
 */
@Component(componentType=ComponentType.REQUESTHANDLERFACTORY)
public class DefaultRequestHandlerFactory extends RequestHandlerFactory {


    /**
     * public constructor to enable loading via the registry
     * @see org.apache.lucene.gdata.server.registry.Component
     * @see org.apache.lucene.gdata.server.registry.GDataServerRegistry
     */
	public DefaultRequestHandlerFactory() {
		//
	}

	/**
	 * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getEntryUpdateHandler()
	 */
	@Override
	public GDataRequestHandler getEntryUpdateHandler() {

		return new DefaultUpdateHandler();
	}

	/**
	 * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getEntryDeleteHandler()
	 */
	@Override
	public GDataRequestHandler getEntryDeleteHandler() {

		return new DefaultDeleteHandler();
	}

	/**
	 * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getFeedQueryHandler()
	 */
	@Override
	public GDataRequestHandler getFeedQueryHandler() {

		return new DefaultGetHandler();
	}

	/**
	 * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getEntryInsertHandler()
	 */
	@Override
	public GDataRequestHandler getEntryInsertHandler() {

		return new DefaultInsertHandler();
	}

    /**
     * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getInsertAccountHandler()
     */
    @Override
    public GDataRequestHandler getInsertAccountHandler() {
        
        return new InsertAccountStrategy();
    }

    /**
     * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getDeleteAccountHandler()
     */
    @Override
    public GDataRequestHandler getDeleteAccountHandler() {
        
        return new DeleteAccountStrategy();
    }

    /**
     * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getUpdateAccountHandler()
     */
    @Override
    public GDataRequestHandler getUpdateAccountHandler() {
        
        return new UpdataAccountStrategy();
    }

    /**
     * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getInsertFeedHandler()
     */
    @Override
    public GDataRequestHandler getInsertFeedHandler() {
        
        return new InsertFeedHandler();
    }

    /**
     * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getUpdateFeedHandler()
     */
    @Override
    public GDataRequestHandler getUpdateFeedHandler() {
        
        return new UpdateFeedHandler();
    }

    /**
     * @see org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory#getDeleteFeedHandler()
     */
    @Override
    public GDataRequestHandler getDeleteFeedHandler() {
        
        return new DeleteFeedHandler();
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#initialize()
     */
    public void initialize() {
        //
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#destroy()
     */
    public void destroy() {
        //
    }

}
