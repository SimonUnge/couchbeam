-module (utils).
-include ("couchbeam.hrl").

-export ([save_doc/1,
          get_id/1,
          get_do/1,
          get_target/1,
          get_field/2,
          get_free_worker/1,
          get_reserved_worker/2,
          get_current_job_step/1,
          get_claim_status/1,
          get_winner_status/1,
          %setters
          set_claim/1,
          increment_step/1,
          set_key_on_doc/3,
          set_executioner/1,
          set_job_step_status/2,
          set_job_step_execution_time/2,
          set_step_start_time/1,
          set_step_finish_time/1,
          set_step_winner/1,
          update_job_step_list/3,
          setnth/3
          ]).

%%Saves the document to the database. If there is a conflict, it does nothing.
save_doc(DocInfo) ->
  case couchbeam:save_doc(DocInfo#document.db, DocInfo#document.doc) of
    {ok, NewDoc} ->
      print("Saving doc id:~p",[DocInfo#document.doc_id]),
      NewDoc;
    {error, conflict} ->
      print("Save conflict!!!!!!!!!!"),
      save_conflict
  end.

%%==== just to have a nicer fuckning print. Hates io:format ====
print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).


%%==== Document Getters and setters =====

%%Getters

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

get_reserved_worker(Workers, DocInfo) ->%XXX är jag dum i huvudet? jag plockar ju ut workers, oavsett status.
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

%%Setters

set_claim(DocInfo) ->
  {_,_,DbId,_} = DocInfo#document.db,
  update_job_step_list(DocInfo, "claimed_by", list_to_binary(DbId)).

set_executioner(DocInfo) ->
  {_,_,DbId,_} = DocInfo#document.db,
  update_job_step_list(DocInfo, "executioner", list_to_binary(DbId)).

set_job_step_execution_time(DocInfo, Time) ->
  update_job_step_list(DocInfo, "exec_time", Time).

set_job_step_status(DocInfo, Status) ->
  update_job_step_list(DocInfo, "step_status", list_to_binary(Status)).

set_step_start_time(DocInfo) ->
  {H, M, S} = time(),
  StartTime = integer_to_list(H) ++ ":" ++ integer_to_list(M) ++ ":" ++ integer_to_list(S),
  update_job_step_list(DocInfo, "start_time", list_to_binary(StartTime)).

set_step_finish_time(DocInfo) ->
  {H, M, S} = time(),
  FinishTime = integer_to_list(H) ++ ":" ++ integer_to_list(M) ++ ":" ++ integer_to_list(S),
  update_job_step_list(DocInfo, "finish_time", list_to_binary(FinishTime)).

set_step_winner(DocInfo) ->
  Winner = get_claim_status(DocInfo),
  DocInfo#document{doc = set_key_on_doc(DocInfo, "creator", Winner).

increment_step(DocInfo) ->
  DocInfo#document{doc = set_key_on_doc(DocInfo,
                                        "step",
                                        DocInfo#document.current_step + 1)
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
                                        Value}
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
