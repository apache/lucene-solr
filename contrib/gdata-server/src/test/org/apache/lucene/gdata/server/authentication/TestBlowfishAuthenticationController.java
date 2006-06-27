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

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.GDataAccount.AccountRole;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 *
 */
public class TestBlowfishAuthenticationController extends TestCase {
    private BlowfishAuthenticationController controller;
    private String key = "myKey";
    private String accountName = "simon";
    
    private String clientIp = "192.168.0.127";
    protected void setUp() throws Exception {
        this.controller = new BlowfishAuthenticationController();
        this.controller.setKey(this.key);
        
        this.controller.initialize();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.server.authentication.AuthenticationController.authenticatAccount(HttpServletRequest)'
     */
    public void testAuthenticatAccount() throws IllegalBlockSizeException, BadPaddingException, AuthenticationException, IOException {
        GDataAccount account = new GDataAccount();
        account.setName(accountName);
        account.setPassword("testme");
        account.setRole(AccountRole.ENTRYAMINISTRATOR);
        
        String token = this.controller.authenticatAccount(account,this.clientIp);
        String notSame = this.controller.calculateAuthToken("192.168.0",Integer.toString(account.getRolesAsInt()),this.accountName);
        assertNotSame(notSame,token);
        String authString = "192.168.0#"+this.accountName +"#"+account.getRolesAsInt()+"#";
        assertTrue(this.controller.deCryptAuthToken(token).startsWith(authString));
        assertTrue(this.controller.deCryptAuthToken(notSame).startsWith(authString));
    }

    /*
     * Test method for 'org.apache.lucene.gdata.server.authentication.AuthenticationController.authenticateToken(String)'
     */
    public void testAuthenticateToken() throws IllegalBlockSizeException, BadPaddingException, UnsupportedEncodingException, AuthenticationException {
        GDataAccount account = new GDataAccount();
        account.setName("simon");
        account.setPassword("testme");
        account.setRole(AccountRole.ENTRYAMINISTRATOR);
        String token = this.controller.calculateAuthToken("192.168.0",Integer.toString(account.getRolesAsInt()),this.accountName);
        
        assertTrue(this.controller.authenticateToken(token,this.clientIp,AccountRole.ENTRYAMINISTRATOR,this.accountName));
        assertTrue(this.controller.authenticateToken(token,this.clientIp,AccountRole.USER,this.accountName));
        assertFalse(this.controller.authenticateToken(token,this.clientIp,AccountRole.USERADMINISTRATOR,"someOtherAccount"));
        try{
        this.controller.authenticateToken(token+"test",this.clientIp,AccountRole.ENTRYAMINISTRATOR,this.accountName);
        fail("exception expected");
        }catch (Exception e) {
            // TODO: handle exception
        }
        this.controller.setMinuteOffset(0);
        assertFalse(this.controller.authenticateToken(token,this.clientIp,AccountRole.ENTRYAMINISTRATOR,this.accountName));
        
    }

    

}
