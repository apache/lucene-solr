package de.lanlab.larm.util;

/**
 * Title: LARM Lanlab Retrieval Machine Description: Copyright: Copyright (c)
 * Company:
 *
 * @author
 * @version   1.0
 */
import java.net.URL;

/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   27. Januar 2002
 */
public class URLUtils
{
    /**
     * does the same as URL.toExternalForm(), but leaves out the Ref part (which we would
     * cut off anyway) and handles the String Buffer so that no call of expandCapacity() will
     * be necessary
     * only meaningful if the default URLStreamHandler is used (as is the case with http, https, or shttp)
     *
     * @param u  the URL to be converted
     * @return   the URL as String
     */
    public static String toExternalFormNoRef(URL u)
    {
        String protocol  = u.getProtocol();
        String authority = u.getAuthority();
        String file      = u.getFile();

        StringBuffer result = new StringBuffer(
                    (protocol == null ? 0 : protocol.length()) +
                    (authority == null ? 0 : authority.length()) +
                    (file == null ? 1 : file.length()) + 3
                    );

        result.append(protocol);
        result.append(":");
        if (u.getAuthority() != null && u.getAuthority().length() > 0)
        {
            result.append("//");
            result.append(u.getAuthority());
        }
        if (u.getFile() != null && u.getFile().length() > 0)
        {
            result.append(u.getFile());
        }
        else
        {
            result.append("/");
        }

        return result.toString();
    }

}
