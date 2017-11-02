%% @author Couchbase <info@couchbase.com>
%% @copyright 2017 Couchbase, Inc.
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

%% @doc methods for handling collections

-module(collections).

-export([get/1,
         update/2]).

get(BucketCfg) ->
    proplists:get_value(collections, BucketCfg, undefined).

update(Bucket, Operation) ->
    {ok, BucketCfg} = ns_bucket:get_bucket(Bucket),
    Manifest = ?MODULE:get(BucketCfg),
    case do_update_collections(Operation, Manifest) of
        {ok, NewManifest} ->
            ok = ns_bucket:update_bucket_config(
                   Bucket,
                   fun (OldConfig) ->
                           lists:keystore(collections, 1, OldConfig, {collections, NewManifest})
                   end);
        {error, Error} ->
            Error
    end.

do_update_collections({add, Name}, Manifest) ->
    add(Name, Manifest);
do_update_collections({delete, Name}, Manifest) ->
    delete(Name, Manifest);
do_update_collections({set_separator, Separator}, Manifest) ->
    set_separator(Separator, Manifest);
do_update_collections({enable, Separator}, Manifest) ->
    enable(Separator, Manifest).

generate_uid() ->
    misc:hexify(crypto:rand_bytes(8)).

add(_Name, undefined) ->
    {error, not_enabled};
add(Name, {Separator, Collections}) ->
    case proplists:get_value(Name, Collections) of
        undefined ->
            {ok, {Separator, lists:ukeysort(1, [{Name, generate_uid()} | Collections])}};
        _ ->
            {error, already_exists}
    end.

delete(_Name, undefined) ->
    {error, not_enabled};
delete(Name, {Separator, Collections}) ->
    case lists:keydelete(Name, 1, Collections) of
        Collections ->
            {error, not_found};
        NewCollections ->
            {ok, {Separator, NewCollections}}
    end.

set_separator(_NewSeparator, undefined) ->
    {error, not_enabled};
set_separator(NewSeparator, {_Separator, []}) ->
    {ok, {NewSeparator, []}};
set_separator(NewSeparator, {_Separator, [{"$default", _}] = C}) ->
    {ok, {NewSeparator, C}};
set_separator(_, _) ->
    {error, has_non_default_collections}.

enable(Separator, undefined) ->
    {ok, {Separator, [{"$default", <<"0">>}]}};
enable(_Separator, _) ->
    {error, already_enabled}.
