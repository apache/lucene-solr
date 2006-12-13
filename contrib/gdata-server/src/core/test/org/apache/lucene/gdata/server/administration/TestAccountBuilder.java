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
package org.apache.lucene.gdata.server.administration;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.GDataAccount.AccountRole;
import org.xml.sax.SAXException;

public class TestAccountBuilder extends TestCase {
    private StringReader reader;
    private String inputXML;
    private StringReader invalidReader;
    private String invalidInputXML;
    protected void setUp() throws Exception {
        this.inputXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<account>" +
                "<account-name>simon</account-name>" +
                "<password>simon</password>" +
                "<account-role>6</account-role>" +
                "<account-owner>" +
                "<name>simon willnauer</name>" +
                "<email-address>simon@gmail.com</email-address>" +
                "<url>http://www.javawithchopsticks.de</url>" +
                "</account-owner>" +
                "</account>";

        this.reader = new StringReader(this.inputXML);
        this.invalidInputXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<account>" +
        "<account-name>simon</account-name>" +
        "<account-role>6</account-role>" +
        "<account-owner>" +
        "<name>simon willnauer</name>" +
        "<email-address>simon@gmail.com</email-address>" +
        "<url>http://www.javawithchopsticks.de</url>" +
        "</account-owner>" +
        "</account>";

        this.invalidReader = new StringReader(this.invalidInputXML);
        
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.server.administration.AdminEntityBuilder.buildUser(Reader)'
     */
    public void testBuildUser() throws IOException, SAXException {
        
        GDataAccount user = AccountBuilder.buildAccount(this.reader);
        assertEquals("simon",user.getName());
        assertEquals("simon willnauer",user.getAuthorname());
        assertEquals("simon@gmail.com",user.getAuthorMail());
        assertEquals("simon",user.getPassword());
        assertEquals(new URL("http://www.javawithchopsticks.de"),user.getAuthorLink());
        assertTrue(user.isUserInRole(AccountRole.ENTRYAMINISTRATOR));
        assertTrue(user.isUserInRole(AccountRole.FEEDAMINISTRATOR));
        assertFalse(user.isUserInRole(AccountRole.USERADMINISTRATOR));
        
    }
    
    public void testBuildUserWrongXML() throws IOException{
        try{
        AccountBuilder.buildAccount(this.invalidReader);
        fail("invalid xml");
        }catch (SAXException e) {
            
        }
    }

}
