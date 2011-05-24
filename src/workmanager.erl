-module (workmanager).
-export ([work_manager/1, setnth/3]).

-record (document, {db, doc, doc_id, current_step, job_length, job_step_do, job_step_list}).

work_manager(N) ->
  Workers = start_workers(N, []),
  print("Length workers ~p", [length(Workers)]),
  work_manager_loop(Workers).

start_workers(N, Workers) when N>0 ->
  process_flag(trap_exit, true),
  WorkerPid = spawn_link(worker, worker, []),
  start_workers(N - 1, [{WorkerPid, free} | Workers]);
start_workers(0, Workers) ->
  Workers.

work_manager_loop(Workers) ->
  receive
    {status, WorkerPid, Doc_Info, Status} ->
      print("Status from worker ~p: ~p",[WorkerPid, Status]),
      NewWorkers = change_worker_status(WorkerPid, Workers, free),
      UpdatedDocInfo = increment_step(Doc_Info),
      save_doc(UpdatedDocInfo),
      work_manager_loop(NewWorkers);
    {changes, Change, Db} ->
      DocId = get_id(Change),
      case couchbeam:open_doc(Db, DocId) of
        {ok, Doc} ->
          CurrentStepNumber = get_field("step", Doc),
          print("The current step: ~p", [CurrentStepNumber]),
          JobStepList = get_field("job", Doc),
          DocInfo = #document{db = Db,
                              doc = Doc,
                              doc_id = DocId,
                              current_step = CurrentStepNumber,
                              job_length = length(JobStepList),
                              job_step_list = JobStepList
                              },
          UpdatedWorkers = give_job_to_worker(Workers, DocInfo),
          work_manager_loop(UpdatedWorkers);
        {error, not_found} ->
          print("Doc deleted?..."),
          work_manager_loop(Workers)
      end
  end.

give_job_to_worker(Workers, DocInfo) ->
  case is_job_complete(DocInfo) of %Bryt ut, gör snyggt.
    false ->
      CurrentJobStep = get_current_job_step(DocInfo#document.job_step_list, DocInfo#document.current_step),
      JobStepDo = get_do(CurrentJobStep),
      print("The do: ~p", [JobStepDo]),
      UpdatedDocInfo = DocInfo#document{job_step_do = JobStepDo},
      NewWorkers = case get_free_worker(Workers) of %%XXX Ny funktion?
                    {WorkerPid, free} ->
                      print("The worker pid: ~p",[WorkerPid]),
                      NewDocInfo = update_job_step_list(UpdatedDocInfo, "claimed_by","ME"),
                      WorkerPid ! {work, self(), NewDocInfo},
                      change_worker_status(WorkerPid, Workers, occupied);
                    false -> 
                      %What to do if no free workers?XXX
                      print("No free workers, what to do?"),
                      Workers
                   end,
      NewWorkers;
    true -> 
      print("Job done"),
      Workers
  end.

%%==== Helpers =====
change_worker_status(WorkerPid,Workers, NewStatus) ->
  lists:keyreplace(WorkerPid, 1, Workers, {WorkerPid, NewStatus}).

is_job_complete(DocInfo) ->
  DocInfo#document.job_length < DocInfo#document.current_step + 1.

is_claimed(DocInfo) ->
  true. %XXX

save_doc(DocInfo) ->
  couchbeam:save_doc(DocInfo#document.db, DocInfo#document.doc).

%%===== Getters =====

get_id(Change) ->
  {Doc} = Change,
  {<<"id">>, ID} = lists:keyfind(<<"id">>, 1, Doc),
  binary_to_list(ID).

%? Merge with get_id?
get_do(CurrentJobStep) ->
  {Step} = CurrentJobStep,
  {<<"do">>, DO} = lists:keyfind(<<"do">>, 1, Step),
  binary_to_list(DO).

get_target(CurrentJobStep) ->
  {Step} = CurrentJobStep,
  {<<"target">>, Target} = lists:keyfind(<<"target">>, 1, Step),
  binary_to_list(Target).

get_field(Field, Doc) ->
  couchbeam_doc:get_value(list_to_binary(Field), Doc).

get_free_worker(Workers) ->
  lists:keyfind(free, 2, Workers).

get_current_job_step(JobStepList, StepNum) -> %XXX Change order of args?
  lists:nth(StepNum + 1, JobStepList). %Erlang list index starts on 1.

%%==== SETTERS ====
set_claim(DocInfo) ->
  true. %XXX

increment_step(DocInfo) ->
  DocInfo#document{doc = set_key_on_doc(DocInfo,
                                        "step",
                                        DocInfo#document.current_step + 1
                                        )
                  }.

set_key_on_doc(DocInfo, Key, Value) ->
  couchbeam_doc:set_value(list_to_binary(Key),
                           Value,
                           DocInfo#document.doc).

%%==== To get job list, change value of key, and return an updated job list, or DocInfo?
update_job_step_list(DocInfo, Key, Value) ->
  {CurrJobStep} = get_current_job_step(DocInfo#document.job_step_list, DocInfo#document.current_step),
  UpdatedCurrJobStep = lists:keyreplace(list_to_binary(Key), 
                                        1, 
                                        CurrJobStep, 
                                        {list_to_binary(Key), 
                                        list_to_binary(Value)}
                                        ),
  Updated_Job_List = setnth(DocInfo#document.current_step + 1, %THis I hate, XXX
                            DocInfo#document.job_step_list, 
                            {UpdatedCurrJobStep}
                            ),
  DocInfo#document{job_step_list = Updated_Job_List,
                   doc = set_key_on_doc(DocInfo, "job", Updated_Job_List)}.
  

%% @spec setnth(Index, List, Element) -> list()
%% @doc Replaces element on index Index with Element
setnth(1, [_|Rest], New) -> [New|Rest];
setnth(I, [E|Rest], New) -> [E|setnth(I-1, Rest, New)].

%%==== just to have a nicer fuckning print. Hates io:format ====
print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).