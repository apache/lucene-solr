//by Michalis Tzikas & Vasilis Lolos
//07-03-2012
//v1.0
/*
Copyright (C) 2011 by Michalis Tzikas & Vasilis Lolos

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
(function( $ ){
  $.fn.linker = function(options) {
    var defaults = {
      target   : '', //blank,self,parent,top
      className : '',
      rel : ''
    };
    var options = $.extend(defaults, options);
        
    target_string = (options.target != '') ? 'target="_'+options.target+'"' : '';
    class_string  = (options.className != '') ? 'class="'+options.className+'"' : '';
    rel_string    = (options.rel != '') ? 'rel="'+options.rel+'"' : '';

    $(this).each(function(){
      t = $(this).text();
      
      t = t.replace(/(https\:\/\/|http:\/\/)([www\.]?)([^\s|<]+)/gi,'<a href="$1$2$3" '+target_string+' '+class_string+' '+rel_string+'>$1$2$3</a>');
      t = t.replace(/([^https\:\/\/]|[^http:\/\/]|^)(www)\.([^\s|<]+)/gi,'$1<a href="http://$2.$3" '+target_string+' '+class_string+' '+rel_string+'>$2.$3</a>');
      t = t.replace(/<([^a]|^\/a])([^<>]+)>/g, "&lt;$1$2&gt;").replace(/&lt;\/a&gt;/g, "</a>").replace(/<(.)>/g, "&lt;$1&gt;").replace(/\n/g, '<br />');

      $(this).html(t);
    });
  };
})( jQuery );