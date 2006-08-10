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
package org.apache.lucene.gdata.data;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.GDataAccount.AccountRole;

/**
 * @author Simon Willnauer
 *
 */
public class TestGDataUser extends TestCase {
    private GDataAccount user;
    @Override
    protected void setUp() throws Exception {
        this.user = new GDataAccount();
        this.user.setName("simonW");
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        
        super.tearDown();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.data.GDataUser.isUserInRole(UserRole)'
     */
    public void testIsUserInRole() {
        assertFalse(this.user.isUserInRole(null));
        assertTrue(this.user.isUserInRole(AccountRole.USER));
        assertFalse(this.user.isUserInRole(AccountRole.ENTRYAMINISTRATOR));
        this.user.setRole(AccountRole.ENTRYAMINISTRATOR);
        assertTrue(this.user.isUserInRole(AccountRole.ENTRYAMINISTRATOR));
    }

    /*
     * Test method for 'org.apache.lucene.gdata.data.GDataUser.getRolesAsInt()'
     */
    public void testGetRolesAsInt() {
        assertEquals(1,this.user.getRolesAsInt());
        this.user.setRole(AccountRole.ENTRYAMINISTRATOR);
        assertEquals(3,this.user.getRolesAsInt());
        this.user.setRole(AccountRole.FEEDAMINISTRATOR);
        assertEquals(7,this.user.getRolesAsInt());
        this.user.setRole(AccountRole.USERADMINISTRATOR);
        assertEquals(15,this.user.getRolesAsInt());
        
        
        
    }
    
    public void testIsUserInRoleInt(){
        assertFalse(GDataAccount.isInRole(1,AccountRole.ENTRYAMINISTRATOR));
        assertFalse(GDataAccount.isInRole(1,AccountRole.FEEDAMINISTRATOR));
        assertTrue(GDataAccount.isInRole(3,AccountRole.ENTRYAMINISTRATOR));
        assertTrue(GDataAccount.isInRole(15,AccountRole.ENTRYAMINISTRATOR));
        assertTrue(GDataAccount.isInRole(3,AccountRole.USER));
        assertTrue(GDataAccount.isInRole(15,AccountRole.USERADMINISTRATOR));
        assertFalse(GDataAccount.isInRole(7,AccountRole.USERADMINISTRATOR));
        assertFalse(GDataAccount.isInRole(7,null));
    }

    /*
     * Test method for 'org.apache.lucene.gdata.data.GDataUser.setRolesAsInt(int)'
     */
    public void testSetRolesAsInt() {
        this.user.setRolesAsInt(2);
        this.user.setRolesAsInt(4);
        this.user.setRolesAsInt(8);
        assertEquals(4,this.user.getRoles().size()); 
        this.user.setRolesAsInt(15);
        assertEquals(4,this.user.getRoles().size());
        this.user = new GDataAccount();
        this.user.setName("simon");
        this.user.setRolesAsInt(15);
        assertEquals(4,this.user.getRoles().size());
        
    }
    
    public void testEquals(){
        assertTrue(this.user.equals(this.user));
        GDataAccount a = new GDataAccount();
        a.setName(this.user.getName());
        assertTrue(this.user.equals(a));
        a.setName("someOtheraccount");
        assertFalse(this.user.equals(a));
        assertFalse(this.user.equals(null));
        assertFalse(this.user.equals(new String()));
        assertFalse(new GDataAccount().equals(new GDataAccount()));
    }
    public void testHashCode(){
        assertEquals(this.user.hashCode(),this.user.hashCode());
        assertFalse(this.user.hashCode()== this.user.getName().hashCode());
        GDataAccount a = new GDataAccount();
        a.setName(this.user.getName());
        assertEquals(this.user.hashCode(),a.hashCode());
        a.setName(null);
        assertFalse(a.hashCode()== this.user.hashCode());
    }
    
    public void testReqValuesSet(){
        assertFalse(this.user.requiredValuesSet());
        this.user.setPassword("hello");
        assertFalse(this.user.requiredValuesSet());
        this.user.setPassword("helloworld");
        assertTrue(this.user.requiredValuesSet());
        assertFalse(new GDataAccount().requiredValuesSet());
    }
    
    public void testToStringPrevNulPEx(){
        assertNotNull(this.user.toString());
    }
    
    

}
