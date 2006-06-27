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

package org.apache.lucene.gdata.utils;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.GDataAccount.AccountRole;
import org.apache.lucene.gdata.server.authentication.AuthenticationController;
import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;

/**
 * @author Simon Willnauer
 *
 */
@Component(componentType=ComponentType.AUTHENTICATIONCONTROLLER)
public class AuthenticationContorllerStub implements AuthenticationController {
    public static AuthenticationController controller;
    /**
     * 
     */
    public AuthenticationContorllerStub() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @see org.apache.lucene.gdata.server.authentication.AuthenticationController#authenticatAccount(org.apache.lucene.gdata.data.GDataAccount, java.lang.String, java.lang.String)
     */
    public String authenticatAccount(GDataAccount account, String requestIp
            ) {

        return controller.authenticatAccount(account,requestIp);
    }

    /**
     * @see org.apache.lucene.gdata.server.authentication.AuthenticationController#authenticateToken(java.lang.String, java.lang.String, org.apache.lucene.gdata.data.GDataAccount.AccountRole, java.lang.String)
     */
    public boolean authenticateToken(String token, String requestIp,
            AccountRole role, String serviceName) {

        return controller.authenticateToken(token,requestIp,role,serviceName);
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#initialize()
     */
    public void initialize() {
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#destroy()
     */
    public void destroy() {
    }

}
