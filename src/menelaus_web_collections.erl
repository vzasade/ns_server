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

%% @doc rest api's for collections

-module(menelaus_web_collections).

-include("ns_common.hrl").

-export([handle_get/2,
         handle_post/2,
         handle_enable/2,
         handle_delete/3,
         handle_put_separator/2]).

handle_get(Bucket, Req) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket),
    case collections:get(BucketConfig) of
        undefined ->
            reply_disabled(Req);
        Collections ->
            menelaus_util:reply_json(Req, jsonify(Collections))
    end.

handle_post(Bucket, Req) ->
    menelaus_util:execute_if_validated(
      fun (Values) ->
              Name = proplists:get_value(name, Values),
              update_collections(Req, Bucket, {add, Name})
      end, Req, validate_post(Req:parse_post())).

validate_post(Args) ->
    R0 = menelaus_util:validate_has_params({Args, [], []}),
    R1 = menelaus_util:validate_required(name, R0),
    R2 = menelaus_util:validate_any_value(name, R1),
    menelaus_util:validate_unsupported_params(R2).

handle_enable(Bucket, Req) ->
    update_collections(Req, Bucket, {enable, ":"}).

handle_delete(Bucket, Name, Req) ->
    update_collections(Req, Bucket, {delete, Name}).

handle_put_separator(Bucket, Req) ->
    menelaus_util:execute_if_validated(
      fun (Values) ->
              Sep = proplists:get_value(separator, Values),
              update_collections(Req, Bucket, {set_separator, Sep})
      end, Req, validate_put_separator(Req:parse_post())).

validate_put_separator(Args) ->
    R0 = menelaus_util:validate_has_params({Args, [], []}),
    R1 = menelaus_util:validate_required(separator, R0),
    R2 = menelaus_util:validate_any_value(separator, R1),
    menelaus_util:validate_unsupported_params(R2).

reply_disabled(Req) ->
    menelaus_util:reply_json(Req, <<"Collections are disabled on this bucket.">>, 404).

jsonify({Separator, Collections}) ->
    {[{separator, list_to_binary(Separator)},
      {collections, [list_to_binary(Name) || {Name, _UID} <- Collections]}]}.

update_collections(Req, Bucket, Oper) ->
    case ns_orchestrator:update_collections(Bucket, Oper) of
        ok ->
            menelaus_util:reply_text(Req, <<>>, 200);
        not_enabled ->
            reply_disabled(Req);
        already_enabled ->
            menelaus_util:reply_json(Req, <<"Already enabled">>, 400);
        already_exists ->
            menelaus_util:reply_json(Req, <<"Collection with this name already exists">>, 400);
        not_found ->
            menelaus_util:reply_json(Req, <<"Collection with this name is not found">>, 404);
        has_non_default_collections ->
            menelaus_util:reply_json(
              Req, <<"Cannot be done if non default collections are present">>, 400);
        Error ->
            menelaus_util:reply_json(
              Req, iolist_to_binary(io_lib:format("Unknown error ~p", [Error])), 400)
    end.
