/*
The MIT License (MIT)

Copyright (c) 2014 Andrey Bezyazychniy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/*
 * Encapsulation of Vassilis Petroulias's base64.js library for AngularJS
 * Original notice included below
 */

/*
 Copyright Vassilis Petroulias [DRDigit]

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
"use strict";angular.module("ab-base64",[]).constant("base64",function(){var a={alphabet:"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=",lookup:null,ie:/MSIE /.test(navigator.userAgent),ieo:/MSIE [67]/.test(navigator.userAgent),encode:function(b){var c,d,e,f,g=a.toUtf8(b),h=-1,i=g.length,j=[,,,];if(a.ie){for(c=[];++h<i;)d=g[h],e=g[++h],j[0]=d>>2,j[1]=(3&d)<<4|e>>4,isNaN(e)?j[2]=j[3]=64:(f=g[++h],j[2]=(15&e)<<2|f>>6,j[3]=isNaN(f)?64:63&f),c.push(a.alphabet.charAt(j[0]),a.alphabet.charAt(j[1]),a.alphabet.charAt(j[2]),a.alphabet.charAt(j[3]));return c.join("")}for(c="";++h<i;)d=g[h],e=g[++h],j[0]=d>>2,j[1]=(3&d)<<4|e>>4,isNaN(e)?j[2]=j[3]=64:(f=g[++h],j[2]=(15&e)<<2|f>>6,j[3]=isNaN(f)?64:63&f),c+=a.alphabet[j[0]]+a.alphabet[j[1]]+a.alphabet[j[2]]+a.alphabet[j[3]];return c},decode:function(b){if(b=b.replace(/\s/g,""),b.length%4)throw new Error("InvalidLengthError: decode failed: The string to be decoded is not the correct length for a base64 encoded string.");if(/[^A-Za-z0-9+\/=\s]/g.test(b))throw new Error("InvalidCharacterError: decode failed: The string contains characters invalid in a base64 encoded string.");var c,d=a.fromUtf8(b),e=0,f=d.length;if(a.ieo){for(c=[];f>e;)c.push(d[e]<128?String.fromCharCode(d[e++]):d[e]>191&&d[e]<224?String.fromCharCode((31&d[e++])<<6|63&d[e++]):String.fromCharCode((15&d[e++])<<12|(63&d[e++])<<6|63&d[e++]));return c.join("")}for(c="";f>e;)c+=String.fromCharCode(d[e]<128?d[e++]:d[e]>191&&d[e]<224?(31&d[e++])<<6|63&d[e++]:(15&d[e++])<<12|(63&d[e++])<<6|63&d[e++]);return c},toUtf8:function(a){var b,c=-1,d=a.length,e=[];if(/^[\x00-\x7f]*$/.test(a))for(;++c<d;)e.push(a.charCodeAt(c));else for(;++c<d;)b=a.charCodeAt(c),128>b?e.push(b):2048>b?e.push(b>>6|192,63&b|128):e.push(b>>12|224,b>>6&63|128,63&b|128);return e},fromUtf8:function(b){var c,d=-1,e=[],f=[,,,];if(!a.lookup){for(c=a.alphabet.length,a.lookup={};++d<c;)a.lookup[a.alphabet.charAt(d)]=d;d=-1}for(c=b.length;++d<c&&(f[0]=a.lookup[b.charAt(d)],f[1]=a.lookup[b.charAt(++d)],e.push(f[0]<<2|f[1]>>4),f[2]=a.lookup[b.charAt(++d)],64!==f[2])&&(e.push((15&f[1])<<4|f[2]>>2),f[3]=a.lookup[b.charAt(++d)],64!==f[3]);)e.push((3&f[2])<<6|f[3]);return e}},b={decode:function(b){b=b.replace(/-/g,"+").replace(/_/g,"/");var c=b.length%4;if(c){if(1===c)throw new Error("InvalidLengthError: Input base64url string is the wrong length to determine padding");b+=new Array(5-c).join("=")}return a.decode(b)},encode:function(b){var c=a.encode(b);return c.replace(/\+/g,"-").replace(/\//g,"_").split("=",1)[0]}};return{decode:a.decode,encode:a.encode,urldecode:b.decode,urlencode:b.encode}}());