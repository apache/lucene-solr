package org.apache.lucene.gdata.data;

import org.apache.lucene.gdata.data.GDataAccount.AccountRole;

import junit.framework.TestCase;

public class TestGDataUser extends TestCase {
    private GDataAccount user;
    @Override
    protected void setUp() throws Exception {
        this.user = new GDataAccount();
        this.user.setName("simon");
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
    
    

}
