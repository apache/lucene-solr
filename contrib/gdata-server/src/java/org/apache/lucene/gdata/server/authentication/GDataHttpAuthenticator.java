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
package org.apache.lucene.gdata.server.authentication;

import javax.servlet.http.HttpServletRequest;

import org.apache.lucene.gdata.data.GDataAccount.AccountRole;
import org.apache.lucene.gdata.server.GDataRequest;

/**
 * The GData protocol is based on the widly know REST approach and therefor
 * client authentication will mostly be provided via a REST interface.
 * <p>
 * This interface describes internally used authentication methods to be
 * implemented by http based authenticator implementations. The GData Server
 * basically has 2 different REST interfaces need authentication. One is for
 * altering feed entries and the other for administration actions.
 * </p>
 * <p>The interface altering entries work with {@link com.google.gdata.client.Service.GDataRequest} object created by the handler and passed to the {@link org.apache.lucene.gdata.server.Service} instance.
 * Administration interfaces use the plain {@link javax.servlet.http.HttpServletRequest} inside the handler.
 * For each type of interface a authentication type a method has to be provided by implementing classes.</p> 
 * 
 * @author Simon Willnauer
 * 
 */
public interface GDataHttpAuthenticator {

    /**
     * Authenticates the client request based on the given GdataRequst and required account role
     * @param request - the gdata request
     * @param role - the required role for passing the authentication
     * 
     * @return <code>true</code> if the request successfully authenticates, otherwise <code>false</code>
     */
    public boolean authenticateAccount(final GDataRequest request,
            AccountRole role);

    /**
     * Authenticates the client request based on the given requst and required account role
     * @param request - the client request
     * @param role - the required role for passing the authentication
     * @return <code>true</code> if the request successfully authenticates, otherwise <code>false</code>
     */
    public boolean authenticateAccount(final HttpServletRequest request,
            AccountRole role);
}
