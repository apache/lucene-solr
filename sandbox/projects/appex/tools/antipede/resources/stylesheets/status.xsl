<?xml version="1.0"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method = "html" encoding="Windows-1252" />
	
	<xsl:template match="status">
<document>
  <header>
    <title>Project Status</title>
  </header>
  
  <body>
    <section title="Project Status">
      <section title="Developers">
      <p>
	     <table>
			 <xsl:for-each select = "developers/person">
			 <tr><td><xsl:value-of select="@name" /></td>
			     <td><xsl:value-of select="@email" /></td>
			     <td><xsl:value-of select="@id" /></td></tr>			 
			 </xsl:for-each>
			 </table>
       </p>
       </section>
       <section title="Changes">
			 <xsl:for-each select = "changes/release">
			 
			  <section> <p>
			    
			    <xsl:attribute  name = "title" >
			    release&#160;<xsl:value-of select = "@version"/>&#160;
			     of date&#160;<xsl:value-of select = "@date"/>
			    </xsl:attribute>

			 <table>
			 
			 <tr><th>type</th><th>what</th><th>developer</th></tr>
			 <xsl:for-each select = "action">
			 <tr><td><xsl:value-of select="@type" /></td>
			     <td><xsl:value-of select="." /></td>
			     <td>[<xsl:value-of select="@dev" />]</td></tr>			 
			 </xsl:for-each>
			 
			 </table>
			 
			 		</p>	  </section> 
			 </xsl:for-each>	

       </section>
              </section>
		 </body></document>
		</xsl:template>
</xsl:stylesheet>