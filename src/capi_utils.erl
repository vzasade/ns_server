%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2011 Couchbase, Inc.
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

-module(capi_utils).

-compile(export_all).

-include("ns_common.hrl").
-include("couch_db.hrl").
-include("mc_entry.hrl").
-include("mc_constants.hrl").
-include("ns_config.hrl").

%% returns capi port for given node or undefined if node doesn't have CAPI
compute_capi_port({ssl, Node}) ->
    ns_config:search('latest-config-marker', {node, Node, ssl_capi_port}, undefined);

compute_capi_port(Node) ->
    ns_config:search('latest-config-marker', {node, Node, capi_port}, undefined).

get_capi_port(Node, Config) ->
    case ns_config:search(Config, {node, Node, capi_port}) of
        false -> undefined;
        {value, X} -> X
    end.

%% returns http url to capi on given node with given path
-spec capi_url_bin(node() | {ssl, node()}, iolist() | binary(), iolist() | binary()) -> undefined | binary().
capi_url_bin(Node, Path, LocalAddr) ->
    case vbucket_map_mirror:node_to_capi_base_url(Node, LocalAddr) of
        undefined -> undefined;
        X ->
            iolist_to_binary([X, Path])
    end.

capi_bucket_url_bin(Node, BucketName, BucketUUID, LocalAddr) ->
    capi_url_bin(Node, menelaus_util:concat_url_path([BucketName ++ "+" ++ ?b2l(BucketUUID)]), LocalAddr).

split_dbname(DbName) ->
    DbNameStr = binary_to_list(DbName),
    Tokens = string:tokens(DbNameStr, [$/]),
    build_info(Tokens, []).

build_info([VBucketStr], R) ->
    {lists:append(lists:reverse(R)), list_to_integer(VBucketStr)};
build_info([H|T], R)->
    build_info(T, [H|R]).

-spec split_dbname_with_uuid(DbName :: binary()) ->
                                    {binary(), binary() | undefined, binary() | undefined}.
split_dbname_with_uuid(DbName) ->
    [BeforeSlash | AfterSlash] = binary:split(DbName, <<"/">>),
    {BucketName, UUID} =
        case binary:split(BeforeSlash, <<"+">>) of
            [BN, U] ->
                {BN, U};
            [BN] ->
                {BN, undefined}
        end,

    {VBucketId, UUID1} = split_vbucket(AfterSlash, UUID),
    {BucketName, VBucketId, UUID1}.

%% if xdcr connects to pre 3.0 clusters we will receive UUID as part of VBucketID
%% use it if no UUID was found in BucketName
split_vbucket([], UUID) ->
    {undefined, UUID};
split_vbucket([AfterSlash], UUID) ->
    case {binary:split(AfterSlash, <<";">>), UUID} of
        {[VB, U], undefined} ->
            {VB, U};
        {[VB, _], UUID} ->
            {VB, UUID};
        {[VB], UUID} ->
            {VB, UUID}
    end.


-spec build_dbname(BucketName :: ext_bucket_name(), VBucket :: ext_vbucket_id()) -> binary().
build_dbname(BucketName, VBucket) ->
    SubName = case is_binary(VBucket) of
                  true -> VBucket;
                  _ -> integer_to_list(VBucket)
              end,
    iolist_to_binary([BucketName, $/, SubName]).


must_open_master_vbucket(BucketName) ->
    DBName = build_dbname(BucketName, <<"master">>),
    case couch_db:open_int(DBName, []) of
        {ok, RealDb} ->
            RealDb;
        Error ->
            exit({open_db_failed, Error})
    end.

couch_json_to_mochi_json({List}) ->
    {struct, couch_json_to_mochi_json(List)};
couch_json_to_mochi_json({K, V}) ->
    {K, couch_json_to_mochi_json(V)};
couch_json_to_mochi_json(List) when is_list(List) ->
    lists:map(fun couch_json_to_mochi_json/1, List);
couch_json_to_mochi_json(Else) -> Else.

couch_doc_to_mochi_json(Doc) ->
    couch_json_to_mochi_json(couch_doc:to_json_obj(Doc, [])).

extract_doc_id(Doc) ->
    Doc#doc.id.

sort_by_doc_id(Docs) ->
    lists:keysort(#doc.id, Docs).

capture_local_master_docs(Bucket, Timeout) ->
    misc:executing_on_new_process(
      fun () ->
              case Timeout of
                  infinity -> ok;
                  _ -> timer2:kill_after(Timeout)
              end,
              DB = must_open_master_vbucket(Bucket),
              {ok, _, LocalDocs} = couch_btree:fold(DB#db.local_docs_btree,
                                                    fun (Doc, _, Acc) ->
                                                            {ok, [Doc | Acc]}
                                                    end, [], []),
              LocalDocs
      end).


-spec fetch_ddoc_ids(bucket_name() | binary()) -> [binary()].
fetch_ddoc_ids(Bucket) ->
    Pairs = foreach_live_ddoc_id(Bucket, fun (_) -> ok end),
    erlang:element(1, lists:unzip(Pairs)).

-spec foreach_live_ddoc_id(bucket_name() | binary(),
                           fun ((binary()) -> any())) -> [{binary(), any()}].
foreach_live_ddoc_id(Bucket, Fun) ->
    Ref = make_ref(),
    RVs = capi_set_view_manager:foreach_doc(
            Bucket,
            fun (Doc) ->
                    case Doc of
                        #doc{deleted = true} ->
                            Ref;
                        _ ->
                            Fun(Doc#doc.id)
                    end
            end, infinity),
    [Pair || {_Id, V} = Pair <- RVs,
             V =/= Ref].

full_live_ddocs(Bucket) ->
    full_live_ddocs(Bucket, infinity).

full_live_ddocs(Bucket, Timeout) ->
    Ref = make_ref(),
    RVs = capi_set_view_manager:foreach_doc(
            Bucket,
            fun (Doc) ->
                    case Doc of
                        #doc{deleted = true} ->
                            Ref;
                        _ ->
                            Doc
                    end
            end, Timeout),
    [V || {_Id, V} <- RVs,
          V =/= Ref].

-spec get_design_doc_signatures(bucket_name() | binary()) -> dict().
get_design_doc_signatures(BucketId) ->
    DesignDocIds = try
                       fetch_ddoc_ids(BucketId)
                   catch
                       exit:{noproc, _} ->
                           []
                   end,

    %% fold over design docs and get the signature
    lists:foldl(
      fun (DDocId, BySig) ->
              {ok, Signature} = couch_set_view:get_group_signature(
                                  mapreduce_view, list_to_binary(BucketId),
                                  DDocId),
              dict:append(Signature, DDocId, BySig)
      end, dict:new(), DesignDocIds).
