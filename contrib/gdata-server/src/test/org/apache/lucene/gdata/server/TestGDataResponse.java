package org.apache.lucene.gdata.server; 
 
import java.io.IOException; 
import java.io.PrintWriter; 
import java.io.StringWriter; 
 
import javax.servlet.http.HttpServletResponse; 
 
import junit.framework.TestCase; 
 
import org.apache.lucene.gdata.server.GDataRequest.OutputFormat; 
import org.easymock.MockControl; 
 
import com.google.gdata.data.Entry; 
import com.google.gdata.data.ExtensionProfile; 
import com.google.gdata.data.Feed; 
import com.google.gdata.data.PlainTextConstruct; 
/** 
 *  
 * @author Simon Willnauer 
 * 
 */ 
public class TestGDataResponse extends TestCase { 
    private GDataResponse response; 
    private HttpServletResponse httpResponse; 
    private MockControl control; 
    private static String generatedFeedAtom = "<?xml version='1.0'?><feed xmlns='http://www.w3.org/2005/Atom' xmlns:openSearch='http://a9.com/-/spec/opensearchrss/1.0/'><entry><title type='text'>Test</title></entry></feed>"; 
    private static String generatedEntryAtom = "<?xml version='1.0'?><entry xmlns='http://www.w3.org/2005/Atom'><title type='text'>Test</title></entry>"; 
    private static String generatedFeedRSS = "<?xml version='1.0'?><rss xmlns:atom='http://www.w3.org/2005/Atom' xmlns:openSearch='http://a9.com/-/spec/opensearchrss/1.0/' version='2.0'><channel><description></description><item><title>Test</title></item></channel></rss>"; 
    private static String generatedEntryRSS = "<?xml version='1.0'?><item xmlns:atom='http://www.w3.org/2005/Atom'><title>Test</title></item>"; 
    protected void setUp() throws Exception { 
        this.control = MockControl.createControl(HttpServletResponse.class); 
        this.httpResponse = (HttpServletResponse)this.control.getMock(); 
        this.response = new GDataResponse(this.httpResponse); 
         
    } 
 
    protected void tearDown() throws Exception { 
        super.tearDown(); 
    } 
     
     
    public void testConstructor(){ 
        try{ 
        new GDataResponse(null); 
        fail("IllegalArgumentExceptin expected"); 
        }catch (IllegalArgumentException e) { 
            // TODO: handle exception 
        } 
    } 
    /* 
     * Test method for 'org.apache.lucene.gdata.server.GDataResponse.sendResponse(BaseFeed, ExtensionProfile)' 
     */ 
    public void testSendResponseBaseFeedExtensionProfile() throws IOException { 
        try{ 
            Feed f = null; 
            this.response.sendResponse(f,new ExtensionProfile()); 
            fail("Exception expected"); 
        }catch (IllegalArgumentException e) { 
            // 
        } 
         
        try{ 
            Feed f = createFeed(); 
            this.response.sendResponse(f,null); 
            fail("Exception expected"); 
        }catch (IllegalArgumentException e) { 
            // 
        } 
        StringWriter stringWriter = new StringWriter(); 
        PrintWriter writer = new PrintWriter(stringWriter); 
         
        this.control.expectAndReturn(this.httpResponse.getWriter(),writer);
        this.httpResponse.setContentType(GDataResponse.XMLMIME_ATOM);
        this.response.setOutputFormat(OutputFormat.ATOM); 
        this.control.replay(); 
         
        this.response.sendResponse(createFeed(),new ExtensionProfile()); 
        assertEquals("Simple XML representation",stringWriter.toString(),generatedFeedAtom); 
        this.control.reset(); 
         
        stringWriter = new StringWriter(); 
        writer = new PrintWriter(stringWriter); 
         
        this.control.expectAndReturn(this.httpResponse.getWriter(),writer); 
        this.response.setOutputFormat(OutputFormat.RSS); 
        this.httpResponse.setContentType(GDataResponse.XMLMIME_RSS);
        this.control.replay(); 
         
        this.response.sendResponse(createFeed(),new ExtensionProfile 
                ()); 
        assertEquals("Simple XML representation",stringWriter.toString(),generatedFeedRSS); 
         
         
         
 
    } 
 
    /* 
     * Test method for 'org.apache.lucene.gdata.server.GDataResponse.sendResponse(BaseEntry, ExtensionProfile)' 
     */ 
    public void testSendResponseBaseEntryExtensionProfile() throws IOException { 
        try{ 
            Entry e = null; 
            this.response.sendResponse(e,new ExtensionProfile()); 
            fail("Exception expected"); 
        }catch (IllegalArgumentException e) { 
            // 
        } 
        try{ 
            Entry e = createEntry(); 
            this.response.sendResponse(e,null); 
            fail("Exception expected"); 
        }catch (IllegalArgumentException e) { 
            // 
        } 
//        // test Atom output 
        StringWriter stringWriter = new StringWriter(); 
        PrintWriter writer = new PrintWriter(stringWriter); 
         
        this.control.expectAndReturn(this.httpResponse.getWriter(),writer); 
        this.response.setOutputFormat(OutputFormat.ATOM); 
        this.control.replay(); 
         
        this.response.sendResponse(createEntry(),new ExtensionProfile 
                ()); 
        assertEquals("Simple XML representation ATOM",stringWriter.toString(),generatedEntryAtom); 
         
        // test rss output 
        this.control.reset(); 
        stringWriter = new StringWriter(); 
        writer = new PrintWriter(stringWriter); 
         
        this.control.expectAndReturn(this.httpResponse.getWriter(),writer); 
        this.response.setOutputFormat(OutputFormat.RSS); 
        this.control.replay(); 
         
        this.response.sendResponse(createEntry(),new ExtensionProfile 
                ()); 
         
        assertEquals("Simple XML representation RSS",stringWriter.toString(),generatedEntryRSS); 
         
         
         
    } 
     
    /* create a simple feed */ 
    private Feed createFeed(){ 
        Feed feed = new Feed(); 
         
        feed.getEntries().add(createEntry()); 
         
        return feed; 
    } 
    /* create a simple entry */ 
    private Entry createEntry(){ 
        Entry e = new Entry(); 
        e.setTitle(new PlainTextConstruct("Test")); 
        return e; 
    } 
 
} 
