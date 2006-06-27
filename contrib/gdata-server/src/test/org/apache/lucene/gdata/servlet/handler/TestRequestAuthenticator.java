package org.apache.lucene.gdata.servlet.handler;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import junit.framework.TestCase;

import org.apache.lucene.gdata.server.authentication.AuthenticationController;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.easymock.MockControl;

public class TestRequestAuthenticator extends TestCase {
    private MockControl requestMock;
    private HttpServletRequest request;
    private RequestAuthenticator authenticator;
    private String tokenHeader;
    private String token;
    private Cookie authCookie;
    
    protected void setUp() throws Exception {
    createMocks();
    this.authenticator = new RequestAuthenticator();
    this.token = "myToken";
    this.tokenHeader = "GoogleLogin auth="+this.token;
    this.authCookie = new Cookie("Auth",this.token);
    }
    protected void createMocks() {
        this.requestMock = MockControl.createControl(HttpServletRequest.class);
        this.request = (HttpServletRequest)this.requestMock.getMock();
        
    }
    protected void tearDown() throws Exception {
        GDataServerRegistry.getRegistry().destroy();
    }
    /*
     * Test method for 'org.apache.lucene.gdata.servlet.handler.RequestAuthenticator.authenticateAccount(GDataRequest, AccountRole)'
     */
    public void testGetTokenFromRequest() {
        // test token present
        this.requestMock.expectAndDefaultReturn(this.request.getHeader(AuthenticationController.AUTHORIZATION_HEADER), this.tokenHeader);
        this.requestMock.replay();
        assertEquals(this.token,this.authenticator.getTokenFromRequest(this.request));
        this.requestMock.verify();
        this.requestMock.reset();
        
        // test token null / cookie present
        this.requestMock.expectAndDefaultReturn(this.request.getHeader(AuthenticationController.AUTHORIZATION_HEADER), null);
        this.requestMock.expectAndDefaultReturn(this.request.getCookies(), new Cookie[]{this.authCookie});
        this.requestMock.replay();
        assertEquals(this.token,this.authenticator.getTokenFromRequest(this.request));
        this.requestMock.verify();
        this.requestMock.reset();
        
        // test token null / cookie not present
        this.requestMock.expectAndDefaultReturn(this.request.getHeader(AuthenticationController.AUTHORIZATION_HEADER), null);
        this.requestMock.expectAndDefaultReturn(this.request.getCookies(), new Cookie[]{new Cookie("somekey","someValue")});
        this.requestMock.replay();
        assertNull(this.authenticator.getTokenFromRequest(this.request));
        this.requestMock.verify();
        this.requestMock.reset();
        
//      test token null / cookie array emtpy 
        this.requestMock.expectAndDefaultReturn(this.request.getHeader(AuthenticationController.AUTHORIZATION_HEADER), null);
        this.requestMock.expectAndDefaultReturn(this.request.getCookies(), new Cookie[]{});
        this.requestMock.replay();
        assertNull(this.authenticator.getTokenFromRequest(this.request));
        this.requestMock.verify();
        this.requestMock.reset();
        
//      test token null / cookie array null
        this.requestMock.expectAndDefaultReturn(this.request.getHeader(AuthenticationController.AUTHORIZATION_HEADER), null);
        this.requestMock.expectAndDefaultReturn(this.request.getCookies(), null);
        this.requestMock.replay();
        assertNull(this.authenticator.getTokenFromRequest(this.request));
        this.requestMock.verify();
        this.requestMock.reset();
    }

}
