<%@include file="header.jsp"%>
<% /* Author: Andrew C. Oliver (acoliver2@users.sourceforge.net) */ %>
<center> 
	<form name="search" action="result.jsp" method="get">
		<p>
			<input name="query" size="44"/>&nbsp;Search Criteria
		</p>
		<p>
			<input name="maxRows" size="4" value="10"/>&nbsp;Results Per Page&nbsp;
			<input type="submit" value="Search"/>
		</p>
        </form>
</center>
<%@include file="footer.jsp"%>