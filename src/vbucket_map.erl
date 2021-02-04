%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2019 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% Access to vbucket maps
%%
-module(vbucket_map).

-include("ns_common.hrl").

-export([key/1,
         get_from_config/2]).

key(Bucket) ->
    ns_bucket:sub_key(Bucket, map).

get_from_config(BucketConfig, Default) ->
    proplists:get_value(map, BucketConfig, Default).
