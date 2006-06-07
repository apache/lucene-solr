package org.apache.lucene.gdata.servlet; 
 
import java.io.IOException; 
 
import javax.servlet.ServletException; 
import javax.servlet.http.HttpServletRequest; 
import javax.servlet.http.HttpServletResponse; 
 
import org.easymock.MockControl; 
 
import junit.framework.TestCase; 
 
/** 
 * @author Simon Willnauer 
 * 
 */ 
public class TestAbstractGdataServlet extends TestCase { 
    private static final String METHOD_DELETE = "DELETE"; 
 
    private static final String METHOD_GET = "GET"; 
 
    private static final String METHOD_POST = "POST"; 
 
    private static final String METHOD_PUT = "PUT"; 
 
    private static final String METHOD_HEADER_NAME = "x-http-method-override"; 
 
    private HttpServletRequest mockRequest = null; 
 
    private HttpServletResponse mockResponse = null; 
 
    private AbstractGdataServlet servletInstance = null; 
 
    private MockControl requestMockControl; 
 
    private MockControl responseMockControl; 
 
    protected void setUp() throws Exception { 
        this.requestMockControl = MockControl 
                .createControl(HttpServletRequest.class); 
        this.responseMockControl = MockControl 
                .createControl(HttpServletResponse.class); 
        this.mockRequest = (HttpServletRequest) this.requestMockControl 
                .getMock(); 
        this.mockResponse = (HttpServletResponse) this.responseMockControl 
                .getMock(); 
        this.servletInstance = new StubGDataServlet(); 
    } 
 
    /** 
     * Test method for 
     * 'org.apache.lucene.gdata.servlet.AbstractGdataServlet.service(HttpServletRequest, 
     * HttpServletResponse)' 
     */ 
    public void testServiceHttpServletRequestHttpServletResponseDelete() { 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getMethod(), METHOD_DELETE); 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getHeader(METHOD_HEADER_NAME), METHOD_DELETE); 
        this.requestMockControl.replay(); 
 
        try { 
            this.servletInstance.service(this.mockRequest, this.mockResponse); 
        } catch (ServletException e) { 
            fail("ServeltExpception not expected"); 
        } catch (IOException e) { 
            fail("IOExpception not expected"); 
        } 
 
        this.requestMockControl.verify(); 
        this.requestMockControl.reset(); 
 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getMethod(), METHOD_POST); 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getHeader(METHOD_HEADER_NAME), METHOD_DELETE); 
        this.requestMockControl.replay(); 
 
        try { 
            this.servletInstance.service(this.mockRequest, this.mockResponse); 
        } catch (ServletException e) { 
            fail("ServeltExpception not expected"); 
        } catch (IOException e) { 
            fail("IOExpception not expected"); 
        } 
 
        this.requestMockControl.verify(); 
    } 
 
    /** 
     *  
     */ 
    public void testServiceNullOverrideHeader() { 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getMethod(), METHOD_POST); 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getHeader(METHOD_HEADER_NAME), null); 
        this.requestMockControl.replay(); 
 
        try { 
            this.servletInstance.service(this.mockRequest, this.mockResponse); 
        } catch (ServletException e) { 
            fail("ServeltExpception not expected"); 
        } catch (IOException e) { 
            fail("IOExpception not expected"); 
        } 
 
        this.requestMockControl.verify(); 
        this.requestMockControl.reset(); 
    } 
 
    /** 
     * Test method for 
     * 'org.apache.lucene.gdata.servlet.AbstractGdataServlet.service(HttpServletRequest, 
     * HttpServletResponse)' 
     */ 
    public void testServiceHttpServletRequestHttpServletResponsePOST() { 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getMethod(), METHOD_POST); 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getHeader(METHOD_HEADER_NAME), METHOD_POST); 
        this.requestMockControl.replay(); 
 
        try { 
            this.servletInstance.service(this.mockRequest, this.mockResponse); 
        } catch (ServletException e) { 
            fail("ServeltExpception not expected"); 
        } catch (IOException e) { 
            fail("IOExpception not expected"); 
        } 
 
        this.requestMockControl.verify(); 
        this.requestMockControl.reset(); 
 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getMethod(), METHOD_PUT); 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getHeader(METHOD_HEADER_NAME), METHOD_POST); 
        this.requestMockControl.replay(); 
 
        try { 
            this.servletInstance.service(this.mockRequest, this.mockResponse); 
        } catch (ServletException e) { 
            fail("ServeltExpception not expected"); 
        } catch (IOException e) { 
            fail("IOExpception not expected"); 
        } 
 
        this.requestMockControl.verify(); 
    } 
 
    /** 
     * Test method for 
     * 'org.apache.lucene.gdata.servlet.AbstractGdataServlet.service(HttpServletRequest, 
     * HttpServletResponse)' 
     */ 
    public void testServiceHttpServletRequestHttpServletResponsePUT() { 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getMethod(), METHOD_PUT); 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getHeader(METHOD_HEADER_NAME), METHOD_PUT); 
        this.requestMockControl.replay(); 
 
        try { 
            this.servletInstance.service(this.mockRequest, this.mockResponse); 
        } catch (ServletException e) { 
            fail("ServeltExpception not expected"); 
        } catch (IOException e) { 
            fail("IOExpception not expected"); 
        } 
 
        this.requestMockControl.verify(); 
        this.requestMockControl.reset(); 
 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getMethod(), METHOD_POST); 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getHeader(METHOD_HEADER_NAME), METHOD_PUT); 
        this.requestMockControl.replay(); 
 
        try { 
            this.servletInstance.service(this.mockRequest, this.mockResponse); 
        } catch (ServletException e) { 
            fail("ServeltExpception not expected"); 
        } catch (IOException e) { 
            fail("IOExpception not expected"); 
        } 
 
        this.requestMockControl.verify(); 
    } 
 
    /** 
     * Test method for 
     * 'org.apache.lucene.gdata.servlet.AbstractGdataServlet.service(HttpServletRequest, 
     * HttpServletResponse)' 
     */ 
    public void testServiceHttpServletRequestHttpServletResponseGET() { 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getMethod(), METHOD_GET); 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getHeader(METHOD_HEADER_NAME), METHOD_GET); 
        this.requestMockControl.replay(); 
 
        try { 
            this.servletInstance.service(this.mockRequest, this.mockResponse); 
        } catch (ServletException e) { 
            fail("ServeltExpception not expected"); 
        } catch (IOException e) { 
            fail("IOExpception not expected"); 
        } 
 
        this.requestMockControl.verify(); 
        this.requestMockControl.reset(); 
 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getMethod(), METHOD_POST); 
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getHeader(METHOD_HEADER_NAME), METHOD_GET); 
        this.requestMockControl.replay(); 
 
        try { 
            this.servletInstance.service(this.mockRequest, this.mockResponse); 
        } catch (ServletException e) { 
            fail("ServeltExpception not expected"); 
        } catch (IOException e) { 
            fail("IOExpception not expected"); 
        } 
 
        this.requestMockControl.verify(); 
 
    } 
    /** 
     * Stub Implementation for <code>AbstractGdataServlet</code> 
     * @author Simon Willnauer 
     * 
     */ 
    static class StubGDataServlet extends AbstractGdataServlet { 
 
        private static final long serialVersionUID = -6271464588547620925L; 
 
        protected void doDelete(HttpServletRequest arg0, 
                HttpServletResponse arg1) { 
            if (arg0.getHeader(METHOD_HEADER_NAME) == null) 
                assertEquals("Http-Method --DELETE--", METHOD_DELETE, arg0 
                        .getMethod()); 
            else 
                assertEquals("Http-Method override --DELETE--", METHOD_DELETE, 
                        arg0.getHeader(METHOD_HEADER_NAME)); 
 
        } 
 
        protected void doGet(HttpServletRequest arg0, HttpServletResponse arg1) { 
            if (arg0.getHeader(METHOD_HEADER_NAME) == null) 
                assertEquals("Http-Method --GET--", arg0.getMethod(), 
                        METHOD_GET); 
            else 
                assertEquals("Http-Method override --GET--", arg0 
                        .getHeader(METHOD_HEADER_NAME), METHOD_GET); 
        } 
 
        protected void doPost(HttpServletRequest arg0, HttpServletResponse arg1) { 
            if (arg0.getHeader(METHOD_HEADER_NAME) == null) 
                assertEquals("Http-Method --POST--", arg0.getMethod(), 
                        METHOD_POST); 
            else 
                assertEquals("Http-Method override --POST--", METHOD_POST, arg0 
                        .getHeader(METHOD_HEADER_NAME)); 
 
        } 
 
        protected void doPut(HttpServletRequest arg0, HttpServletResponse arg1) { 
            if (arg0.getHeader(METHOD_HEADER_NAME) == null) 
                assertEquals("Http-Method --PUT--", arg0.getMethod(), 
                        METHOD_PUT); 
            else 
                assertEquals("Http-Method override --PUT--", arg0 
                        .getHeader(METHOD_HEADER_NAME), METHOD_PUT); 
        } 
 
    } 
 
} 
