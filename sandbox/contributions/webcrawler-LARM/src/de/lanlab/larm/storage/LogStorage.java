package de.lanlab.larm.storage;

import de.lanlab.larm.util.WebDocument;
import de.lanlab.larm.util.SimpleLogger;
import java.io.*;


/**
 * Title: LARM Lanlab Retrieval Machine Description: Copyright: Copyright (c)
 * Company:
 *
 * @author
 * @created   11. Januar 2002
 * @version   1.0
 */



/**
 * this class saves the documents into page files of 50 MB and keeps a record of all
 * the positions into a Logger. the log file contains URL, page file number, and
 * index within the page file.
 *
 */

public class LogStorage implements DocumentStorage
{

    SimpleLogger log;

    File pageFile;
    FileOutputStream out;
    int pageFileCount;
    String filePrefix;
    int offset;
    boolean isValid = false;
    /**
     * Description of the Field
     */
    public final static int MAXLENGTH = 50000000;
    boolean logContents = false;
    String fileName;


    /**
     * Constructor for the LogStorage object
     *
     * @param log          the logger where index information is saved to
     * @param logContents  whether all docs are to be stored in page files or not
     * @param filePrefix   the file name where the page file number is appended
     */
    public LogStorage(SimpleLogger log, boolean logContents, String filePrefix)
    {
        this.log = log;
        pageFileCount = 0;
        this.filePrefix = filePrefix;
        this.logContents = logContents;
        if (logContents)
        {
            openPageFile();
        }
    }


    /**
     * Description of the Method
     */
    public void open() { }


    /**
     * Description of the Method
     */
    public void openPageFile()
    {
        int id = ++pageFileCount;
        fileName = filePrefix + "_" + id + ".pfl";
        try
        {
            this.offset = 0;
            out = new FileOutputStream(fileName);
            isValid = true;
        }
        catch (IOException io)
        {
            log.logThreadSafe("**ERROR: IOException while opening pageFile " + fileName + ": " + io.getClass().getName() + "; " + io.getMessage());
            isValid = false;
        }
    }


    /**
     * Gets the outputStream attribute of the LogStorage object
     *
     * @return   The outputStream value
     */
    public OutputStream getOutputStream()
    {
        if (offset > MAXLENGTH)
        {
            try
            {
                out.close();
            }
            catch (IOException io)
            {
                log.logThreadSafe("**ERROR: IOException while closing pageFile " + fileName + ": " + io.getClass().getName() + "; " + io.getMessage());
            }
            openPageFile();
        }
        return out;
    }


    /**
     * Description of the Method
     *
     * @param bytes  Description of the Parameter
     * @return       Description of the Return Value
     */
    public synchronized int writeToPageFile(byte[] bytes)
    {
        try
        {
            OutputStream out = getOutputStream();
            int oldOffset = this.offset;
            out.write(bytes);
            this.offset += bytes.length;
            return oldOffset;
        }
        catch (IOException io)
        {
            log.logThreadSafe("**ERROR: IOException while writing " + bytes.length + " bytes to pageFile " + fileName + ": " + io.getClass().getName() + "; " + io.getMessage());
        }
        return -1;
    }


    /**
     * Sets the logger attribute of the LogStorage object
     *
     * @param log  The new logger value
     */
    public void setLogger(SimpleLogger log)
    {
        this.log = log;
    }


    /**
     * stores the document if storing is enabled
     *
     * @param doc  Description of the Parameter
     */
    public void store(WebDocument doc)
    {
        String docInfo = doc.getInfo();
        if (logContents && isValid && doc.getDocumentBytes() != null)
        {
            int offset = writeToPageFile(doc.getDocumentBytes());
            docInfo = docInfo + "\t" + pageFileCount + "\t" + offset;
        }
        log.logThreadSafe(docInfo);
    }
}
