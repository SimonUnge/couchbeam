-module (worker).
-export ([worker/0]).

-record (document, {db, doc, doc_id, current_step, job_length, job_step_do, job_step_list}).

worker() ->
  receive
    {work, From, Doc_Info} ->
      Retries = 3,
      Status = do_work(Doc_Info, Retries),
      From ! {status, self(), Doc_Info, Status},
      worker()
  end.

do_work(Doc_Info, Retries) ->
  P = open_port({spawn, Doc_Info#document.job_step_do}, [exit_status]),
  case get_status(P) of
    {exit_status, Status} when Status =:= 0 ->
      Status;
    {exit_status, _Status} when Retries > 0 ->
      do_work(Doc_Info, Retries - 1);
    {exit_status, Status} ->
      Status
  end. 

get_status(P) ->
  receive
    {P, {exit_status, Status}} ->
      {exit_status, Status};
    {P, Any} ->
      io:format("This is what I got: ~p ~n", [Any]),
      get_status(P)
  end.

%%== just to have a nicer fuckning print. Hates io:format
print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).