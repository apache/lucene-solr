package de.lanlab.larm.fetcher;

import java.net.*;
import java.io.*;
import de.lanlab.larm.util.URLUtils;

/**
 * represents a URL which is passed around in the messageHandler
 */
public class URLMessage implements Message, Serializable
{
    /**
     * the URL
     */
    protected URL url;
    protected String urlString;

    protected URL referer;
    protected String refererString;
    boolean isFrame;

    public URLMessage(URL url, URL referer, boolean isFrame)
    {
        //super();
        this.url = url;
        this.urlString = url != null ? URLUtils.toExternalFormNoRef(url) : null;

        this.referer = referer;
        this.refererString = referer != null ? URLUtils.toExternalFormNoRef(referer) : null;
        this.isFrame = isFrame;
        //System.out.println("" + refererString + " -> " + urlString);
    }

    public URL getUrl()
    {
        return this.url;
    }

    public URL getReferer()
    {
        return this.referer;
    }


    public String toString()
    {
        return urlString;
    }

    public String getURLString()
    {
        return urlString;
    }

    public String getRefererString()
    {
        return refererString;
    }


    public int hashCode()
    {
        return url.hashCode();
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException
    {
        out.writeObject(url);
        out.writeObject(referer);
        out.writeBoolean(isFrame);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        url = (URL)in.readObject();
        referer = (URL)in.readObject();
        urlString = url.toExternalForm();
        refererString = referer.toExternalForm();
        isFrame = in.readBoolean();
    }

    public String getInfo()
    {
        return (referer != null ? refererString : "<start>") + "\t" + urlString + "\t" + (isFrame ? "1" : "0");
    }

}
