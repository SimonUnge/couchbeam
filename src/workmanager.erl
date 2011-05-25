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
  start_workers(N - 1, [{WorkerPid, free, null} | Workers]);
start_workers(0, Workers) ->
  Workers.

work_manager_loop(Workers) ->
  receive
    {status, WorkerPid, Doc_Info, Status} ->
      print("Status from worker ~p: ~p",[WorkerPid, Status]),
      NewWorkers = change_worker_status(WorkerPid, Workers, free, null),
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
          print("Worker status list: ~p", [Workers]),
          UpdatedWorkers = handle_job(Workers, DocInfo),
          work_manager_loop(UpdatedWorkers);
        {error, not_found} ->
          print("Doc deleted?..."),
          work_manager_loop(Workers)
      end
  end.

handle_job(Workers, DocInfo) ->
  case is_job_complete(DocInfo) of
    false -> %There are still steps that have not been executed.
      inspect_winner_and_handle(Workers, DocInfo);
    true ->
      print("Job done"),
      Workers
  end.

inspect_winner_and_handle(Workers, DocInfo) ->
  case has_winner(DocInfo) of
    true ->
      print("Job have a winner"),
      handle_has_winner(Workers, DocInfo);
    false ->
    %kolla om det är till alla,
      print("Job does not have a winner"),
      inspect_claim_and_handle(Workers, DocInfo)
  end.

handle_has_winner(Workers, DocInfo) ->
  case is_winner(DocInfo) of
    true ->
      print("I am winner"),
      %nu ska det det finnas en worker reserverat för detta step, om det inte har krashat?
      %SKA Testar att ge den en process. Farligt? XXX
      give_job_to_reserved_worker(Workers, DocInfo);
    false ->
      %frigör eventuell bokad worker.
      print("I am not winner"),
      release_worker(Workers, DocInfo)
  end.

inspect_claim_and_handle(Workers, DocInfo) ->
  case is_claimed(DocInfo) of
    true ->
      print("Step is claimed already"),
      Workers;
    false ->
      print("Step is not claimed, checking workers etc..."),
      inspect_target_and_handle(Workers, DocInfo)
  end.  

inspect_target_and_handle(Workers, DocInfo) ->
  case is_target_any(DocInfo) of
    true ->
      print("Any is target"),
      handle_any_target(Workers, DocInfo);
    false ->
      print("there is a specific target"),
      handle_specific_target(Workers, DocInfo)
  end.

handle_any_target(Workers, DocInfo) ->
  case has_free_workers(Workers) of
    true ->
      print("there are free workers, setting claim"),
      UpdatedDocInfo = set_claim(DocInfo),
      UpdatedWorkers = book_worker(Workers, UpdatedDocInfo),
      save_doc(UpdatedDocInfo),
      UpdatedWorkers;
    false ->
      Workers
  end.

handle_specific_target(Workers, DocInfo) ->
  case is_target_me(DocInfo) of
    true ->
      print("I am target"),
      handle_me_target(Workers, DocInfo);
    false ->
      print("I am not target..."),
      Workers
  end.

handle_me_target(Workers, DocInfo) ->
  case has_free_workers(Workers) of
    true ->
      print("I am target and has workers"),
      give_job_to_worker(Workers, DocInfo);
    false ->
      print("I am target but has no workers"),
      Workers
  end.
%%==== Worker Helpers =====

give_job_to_worker(Workers, DocInfo) ->
  CurrentJobStep = get_current_job_step(DocInfo),
  JobStepDo = get_do(CurrentJobStep),
  print("The do: ~p", [JobStepDo]),
  UpdatedDocInfo = DocInfo#document{job_step_do = JobStepDo},
  {WorkerPid, free, _DocId} = get_free_worker(Workers), 
  print("The worker pid: ~p",[WorkerPid]),
  WorkerPid ! {work, self(), UpdatedDocInfo},
  change_worker_status(WorkerPid, Workers, busy, UpdatedDocInfo#document.doc_id).

give_job_to_reserved_worker(Workers, DocInfo) ->
  CurrentJobStep = get_current_job_step(DocInfo),
  JobStepDo = get_do(CurrentJobStep),
  print("The do: ~p", [JobStepDo]),
  UpdatedDocInfo = DocInfo#document{job_step_do = JobStepDo},
  Doc_Id = UpdatedDocInfo#document.doc_id,
  {WorkerPid, booked, Doc_Id} = get_reserved_worker(Workers, UpdatedDocInfo),
  WorkerPid ! {work, self(), UpdatedDocInfo},
  change_worker_status(WorkerPid, Workers, busy, UpdatedDocInfo#document.doc_id).
  

change_worker_status(WorkerPid, Workers, NewStatus, DocId) ->
  lists:keyreplace(WorkerPid, 1, Workers, {WorkerPid, NewStatus, DocId}).

book_worker(Workers, DocInfo) ->
  {WorkerPid, free, _DocId} = get_free_worker(Workers),
  print("booking worker ~p", [WorkerPid]),
  change_worker_status(WorkerPid, Workers, booked, DocInfo#document.doc_id).

release_worker(Workers, DocInfo) ->
  Doc_Id = DocInfo#document.doc_id,
  case get_reserved_worker(Workers, DocInfo) of
    {WorkerPid, booked, Doc_Id} ->
      print("Release worker: ~p", [WorkerPid]),
      change_worker_status(WorkerPid, Workers, free, null);
    false ->
      print("Ah, I did not book this job"),
      Workers
  end.

%%=========================  

is_job_complete(DocInfo) ->
  DocInfo#document.job_length < DocInfo#document.current_step + 1.

%Returns true if the job-step is claimed.
is_claimed(DocInfo) ->
  ClaimStatus = get_claim_status(DocInfo),
  null =/= ClaimStatus.

has_winner(DocInfo) ->
  WinnerStatus = get_winner_status(DocInfo),
  WinnerStatus =/= null.

is_winner(DocInfo) ->
  WinnerStatus = get_winner_status(DocInfo),
  {_,_,DbId,_} = DocInfo#document.db,
  WinnerStatus =:= list_to_binary(DbId).

is_target_any(DocInfo) ->
  Target = get_target(DocInfo),
  Target =:= "any".

is_target_me(DocInfo) ->
  Target = get_target(DocInfo),
  {_,_,DbId,_} = DocInfo#document.db,
  Target =:= DbId.

has_free_workers(Workers) ->
  case get_free_worker(Workers) of
    false ->
      false;
    _Worker ->
      true
  end.

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

get_target(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"target">>, Target} = lists:keyfind(<<"target">>, 1, CurrentJobStep),
  binary_to_list(Target).

get_field(Field, Doc) ->
  couchbeam_doc:get_value(list_to_binary(Field), Doc).

get_free_worker(Workers) ->
  lists:keyfind(free, 2, Workers).

get_reserved_worker(Workers, DocInfo) ->
  lists:keyfind(DocInfo#document.doc_id, 3, Workers).

get_current_job_step(DocInfo) -> %XXX Change order of args?
  lists:nth(DocInfo#document.current_step + 1, DocInfo#document.job_step_list). %Erlang list index starts on 1.

get_claim_status(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"claimed_by">>, ClaimStatus} = lists:keyfind(<<"claimed_by">>, 1, CurrentJobStep),
  ClaimStatus.

get_winner_status(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"winner">>, WinnerStatus} = lists:keyfind(<<"winner">>, 1, CurrentJobStep),
  WinnerStatus.

%%==== SETTERS ====
set_claim(DocInfo) ->
  {_,_,DbId,_} = DocInfo#document.db,
  update_job_step_list(DocInfo, "claimed_by", DbId).

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
  {CurrJobStep} = get_current_job_step(DocInfo),
  UpdatedCurrJobStep = lists:keyreplace(list_to_binary(Key),
                                        1,
                                        CurrJobStep,
                                        {list_to_binary(Key),
                                        list_to_binary(Value)}
                                       ),
  Updated_Job_List = setnth(DocInfo#document.current_step + 1, %THis I hate, + 1, XXX
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