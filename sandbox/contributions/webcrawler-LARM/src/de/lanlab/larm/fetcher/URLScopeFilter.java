package de.lanlab.larm.fetcher;

import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Pattern;

/**
 * Filter-Klasse; prüft eine eingegangene Message auf Einhaltung eines
 * regulären Ausdrucks. Wenn die URL diesem Ausdruck
 * nicht entspricht, wird sie verworfen
 * @author Clemens Marschner
 */
class URLScopeFilter extends Filter implements MessageListener
{
    public void notifyAddedToMessageHandler(MessageHandler handler)
    {
      this.messageHandler = handler;
    }
    MessageHandler messageHandler;

    /**
	 * the regular expression which describes a valid URL
	 */
    private Pattern pattern;
    private Perl5Matcher matcher;
    private Perl5Compiler compiler;

    public URLScopeFilter()
    {
            matcher = new Perl5Matcher();
            compiler = new Perl5Compiler();
    }

    public String getRexString()
    {
        return pattern.toString();
    }

	/**
	 * set the regular expression
	 * @param rexString the expression
	 */
    public void setRexString(String rexString) throws org.apache.oro.text.regex.MalformedPatternException
    {
        this.pattern = compiler.compile(rexString, Perl5Compiler.CASE_INSENSITIVE_MASK | Perl5Compiler.SINGLELINE_MASK);
        //System.out.println("pattern set to: " + pattern);
    }


    /**
     * this method will be called by the message handler. Tests the URL
	 * and throws it out if it's not in the scope
     */
	public Message handleRequest(Message message)
	{
	    if(message instanceof URLMessage)
	    {
	        String urlString = ((URLMessage)message).toString();
	        int length = urlString.length();
	        char buffer[] = new char[length];
	        urlString.getChars(0,length,buffer,0);

            //System.out.println("using pattern: " + pattern);
	        boolean match = matcher.matches(buffer, pattern);
	        if(!match)
	        {
	            //System.out.println("not in Scope: " + urlString);
                filtered++;
	            return null;
	        }
	    }
        return message;
	}

}