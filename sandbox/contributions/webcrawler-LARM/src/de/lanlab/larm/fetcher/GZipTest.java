package de.lanlab.larm.fetcher;

/**
 * Title: LARM Lanlab Retrieval Machine Description: Copyright: Copyright (c)
 * Company:
 *
 * @author
 * @version   1.0
 */

import java.io.*;
import java.util.zip.*;
import java.net.*;

/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   28. Januar 2002
 */
public class GZipTest
{

    /**
     * Constructor for the GZipTest object
     */
    public GZipTest() { }


    /**
     * The main program for the GZipTest class
     *
     * @param args  The command line arguments
     */
    public static void main(String[] args)
    {
        try
        {
            String url = "http://speechdat.phonetik.uni-muenchen.de/speechdt//speechDB/FIXED1SL/BLOCK00/SES0006/A10006O5.aif";

            ByteArrayOutputStream a = new ByteArrayOutputStream(url.length());
            GZIPOutputStream g = new GZIPOutputStream(a);
            OutputStreamWriter o = new OutputStreamWriter(g,"ISO-8859-1");

            o.write(url);
            o.close();
            g.finish();
            byte[] array = a.toByteArray();
            System.out.println("URL: " + url + " \n Length: " + url.length() + "\n zipped: " + array.length
                    );
        }
        catch (Exception e)
        { e.printStackTrace();
        }
    }
}
