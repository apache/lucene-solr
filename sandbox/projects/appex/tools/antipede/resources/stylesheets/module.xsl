<?xml version="1.0"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method = "html" encoding="Windows-1252" />
	
	<xsl:template match="/">	
		<html>
			<head>
			  <title>Gump descriptor of module <xsl:value-of select="module/@name" /></title>
			</head>
			<body>
			
			 <h1><xsl:value-of select="module/@name" /></h1>

			 <p>website:<a><xsl:attribute  name = "href" >
 			        <xsl:value-of select="module/url/@href" />
			       </xsl:attribute><xsl:value-of select="module/url/@href" /></a>  
			 <br/>cvs repository: <xsl:value-of select="module/cvs/@repository" />
			 <xsl:for-each select = "module/mailing-lists/mailing-list">
			 <br/><xsl:value-of select="@user" />&#160;mailing list:&#160; 
			      <a><xsl:attribute  name = "href" >mailto:<xsl:value-of select="@mail" /></xsl:attribute>
			         <xsl:value-of select="@mail" /></a>
			      <a><xsl:attribute  name = "href" >mailto:<xsl:value-of select="@subscribe" /></xsl:attribute>
			         Subscribe</a>  
                  <a><xsl:attribute  name = "href" >mailto:<xsl:value-of select="@unsubscribe" /></xsl:attribute>
                     Unsubscribe</a>  			         
			       			 
			 </xsl:for-each>
             </p>
			 			 
			 <h2>Description</h2>
			 <p><xsl:value-of select="module/description" /></p>
			 <p><xsl:value-of select="module/detailed" /></p>

			 <h2>Reasons</h2>
			 <p><xsl:value-of select="module/why" /></p>
			 
			 <h2>Goals</h2>			 	
			 <ul>
			 <xsl:for-each select = "module/what/goal">
			 <li><xsl:value-of select="." /></li>			 
			 </xsl:for-each>
			 </ul>

			 <h2>License</h2>
			 <p><b><xsl:value-of select="module/licence" /></b></p>
			 
			 <h2>Credits</h2>			 	
			 <ul>
			 <xsl:for-each select = "module/credits/credit">
			 <li><xsl:value-of select="." /></li>			 
			 </xsl:for-each>
			 </ul>			 

			 </body>
		</html>
	</xsl:template>
</xsl:stylesheet>