-module (keepdocsalive).
-export ([keep_doc_alive/1]).
-include("couchbeam.hrl").


keep_docs_alive(ViewObj, Db) ->
  timer:sleep(5000),
  UnclaimedList = get_unclaimed_list(ViewObj),
  re_save_docs(UnclaimedList, Db),
  keep_docs_alive(ViewObj).

get_unclaimed_list(ViewObj) ->
  {ok, {ViewList}} = couchbeam_view:fetch(ViewObj),
  {<<"rows">>, UnclaimedList} = lists:keyfind(<<"rows">>, 1, ViewList),
  UnclaimedList.

re_save_docs([], _Db) ->
  ok;  
re_save_docs([{H} | T], Db) ->
  {<<"id">>, DocId} = lists:keyfind(<<"id">>, 1, H),
  {ok, Doc} = couchbeam:open_doc(Db, binary_to_list(DocId)),
  couchbeam:save_doc(Db, Doc),
  re_save_docs(T).