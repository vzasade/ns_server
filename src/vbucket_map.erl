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
         key_filter/0,
         key_filter/1,
         get/1,
         get/2,
         get_with_default/2,
         get_from_config/2]).

key(Bucket) ->
    ns_bucket:sub_key(Bucket, map).

%% empty for now, since {bucket, _, map} keys automatically appear in
%% bucket related snapshots
key_filter() ->
    [].

key_filter(_Bucket) ->
    [].

get(Bucket) ->
    get(Bucket, direct).

get(Bucket, Snapshot) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket, Snapshot),
    Map = get_from_config(BucketConfig, undefined),
    true = Map =/= undefined,
    Map.

get(Bucket, Snapshot, Default) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket, Snapshot),
    get_from_config(BucketConfig, []).

get_from_config(BucketConfig, Default) ->
    proplists:get_value(map, BucketConfig, Default).
