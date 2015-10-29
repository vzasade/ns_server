/**
   Copyright 2015 Couchbase, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 **/

var FtsSection = {
  init: function () {
     var port = 9200;
     jQuery.ajax({
        url:    '/nodes/self/ftsPort',
        success: function(result) {
            port = result["ftsPort"];
        },
        async:   false
      });
     ifrm = document.createElement("IFRAME");
     ifrm.setAttribute("src", "http://localhost:"+ port);
     ifrm.setAttribute("id", "fts_iframe");
     ifrm.style.width = 950+"px";
     ifrm.style.height = 800+"px";
     document.getElementById("js_fts").appendChild(ifrm);
  },
  onEnter: function () {
    var iframe = document.getElementById('fts_iframe');
    iframe.src = iframe.src;
  },
  navClick: function () {
  }
};
