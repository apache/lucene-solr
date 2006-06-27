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

}
