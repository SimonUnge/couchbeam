-module (keepdocalive).
-export ([keep_doc_alive/1]).

-record (document, {db, doc, doc_id, current_step, job_length, job_step_do, job_step_list, sleep_time}).

keep_doc_alive(DocInfo) ->
  io:format("IM KEEP ALIVE; DO I EVEN GET HERE~n"),
  io:format("keep alive ~p got a mission, save doc id: ~p ~n",[self(), DocInfo#document.doc_id]),
  timer:sleep(5000),
  workmanager:save_doc(DocInfo).
