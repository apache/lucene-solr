/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

package de.lanlab.larm.storage;
import java.sql.*;
import de.lanlab.larm.util.*;
import java.util.*;

/**
 * saves the document into an sql table. At this time only in MS SQL (and probably Sybase)
 * a table "Document" with the columns DO_URL(varchar), DO_MimeType(varchar) and
 * DO_Data2(BLOB) is created after start<br>
 * notes: experimental; slow
 */
public class SQLServerStorage implements DocumentStorage
{

    private Vector freeCons;
    private Vector busyCons;

    private Vector freeStatements;
    private Vector busyStatements;

    private PreparedStatement addDoc;

    public SQLServerStorage(String driver, String connectionString, String account, String password, int nrConnections)
    {
        try
        {
            Class.forName(driver);
            freeCons = new Vector(nrConnections);
            busyCons = new Vector(nrConnections);
            freeStatements = new Vector(nrConnections);
            busyStatements = new Vector(nrConnections);

            Connection sqlConn;
            PreparedStatement statement;
            for(int i=0; i<nrConnections; i++)
            {
                sqlConn = DriverManager.getConnection(connectionString, account, password);
                statement = sqlConn.prepareStatement("INSERT INTO Document (DO_URL, DO_MimeType, DO_Data2) VALUES (?,?,?)");
                freeCons.add(sqlConn);
                freeStatements.add(statement);
            }



        }
        catch(SQLException e)
        {
            synchronized(this)
            {
                System.out.println(/*"Task " + taskNr + ": */ "SQLException: " + e.getMessage());
                System.err.println("       SQLState:     " + e.getSQLState());
                System.err.println("       VendorError:  " + e.getErrorCode());
            }
            return;
        }

        catch(Exception e)
        {
            System.out.println("SQLServerStorage: " + e.getClass().getName() + ": " + e.getMessage());
            e.printStackTrace();
            System.exit(0);
        }
    }

    public Connection getConnection()
    {
        synchronized(this)
        {
            Connection actual = (Connection)freeCons.firstElement();
            freeCons.removeElementAt(0);
            if(actual == null)
            {
                return null;
            }
            busyCons.add(actual);
            return actual;
        }
    }

    public void releaseConnection(Connection con)
    {
        synchronized(this)
        {
            busyCons.remove(con);
            freeCons.add(con);
        }
    }

    public PreparedStatement getStatement()
    {
        synchronized(this)
        {
            PreparedStatement actual = (PreparedStatement)freeStatements.firstElement();
            freeStatements.removeElementAt(0);
            if(actual == null)
            {
                return null;
            }
            busyStatements.add(actual);
            return actual;
        }
    }

    public void releaseStatement(PreparedStatement statement)
    {
        synchronized(this)
        {
            busyStatements.remove(statement);
            freeStatements.add(statement);
        }
    }

    public void open()
    {
        Connection conn = null;
        try
        {
            conn = getConnection();
            Statement delDoc = conn.createStatement();

            // bisherige Daten löschen, indem die Tabelle neu angelegt wird (geht schneller)

            delDoc.executeUpdate("if exists (select * from sysobjects where id = object_id(N'[dbo].[Document]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)drop table [dbo].[Document]");
            delDoc.executeUpdate("CREATE TABLE [dbo].[Document] ([DO_ID] [int] IDENTITY (1, 1) NOT NULL ,	[DA_CrawlPass] [int] NULL ,	[DO_URL] [varchar] (255) NULL ,	[DO_ContentType] [varchar] (50) NULL ,	[DO_Data] [text] NULL ,	[DO_Hashcode] [int] NULL ,	[DO_ContentLength] [int] NULL ,	[DO_ContentEncoding] [varchar] (20) NULL ,	[DO_Data2] [image] NULL, [DO_MimeType] [varchar] (255) NULL) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]");       // löschen
        }
        catch(SQLException e)
        {
            System.out.println(/*"Task " + taskNr + ": */"SQLException: " + e.getMessage());
            System.err.println("       SQLState:     " + e.getSQLState());
            System.err.println("       VendorError:  " + e.getErrorCode());
        }
        finally
        {
            if(conn != null)
            {
                releaseConnection(conn);
            }
        }
    }

    public void store(WebDocument document)
    {

        PreparedStatement addDoc = null;
        try
        {
            addDoc = getStatement();
            addDoc.setString(1, document.getURLString());
            addDoc.setString(2, document.getMimeType());
            addDoc.setBytes(3,  document.getDocumentBytes());
            addDoc.execute();
        }
        catch(SQLException e)
        {
            System.out.println(/* "Task " + taskNr + ": */ "SQLException: " + e.getMessage());
            System.err.println("       SQLState:     " + e.getSQLState());
            System.err.println("       VendorError:  " + e.getErrorCode());
        }
        finally
        {
            if(addDoc != null)
            {
                releaseStatement(addDoc);
            }
        }
    }
}
