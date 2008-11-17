/**
@constructor
*/
function Layout(p) {
	/** initilize 1 */
	this.init = function(p) {
	}
	
	/** get the id */
	this.getId = function() {
	}
	
	/** @type string */
	this.orientation = "landscape";
	
	function getInnerElements(elementSecretId){
	}
}

/**
@constructor
@borrows Layout#orientation as #orientation
@borrows Layout-getInnerElements as myGetInnerElements
*/
function Page() {
	/** reset the page */
	this.reset = function(b) {
	}
}

/**
@constructor
@borrows Layout.prototype.orientation as this.orientation
@borrows Layout.prototype.init as #init
@inherits Page.prototype.reset as #reset
*/
function ThreeColumnPage() {
	/** initilize 2 */
	this.init = function(p) {
	}
}
