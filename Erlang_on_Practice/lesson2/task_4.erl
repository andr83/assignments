-module(task_4).

-export([dropwhile/2, takewhile/2]).

-include_lib("eunit/include/eunit.hrl").


%% implement lists:dropwhile/2
%% http://www.erlang.org/doc/man/lists.html#dropwhile-2
dropwhile(_, []) -> [];

dropwhile(Pred, List) ->
    %% BEGIN
    [Head|Tail] = List,
    case Pred(Head) of
      true -> dropwhile(Pred, Tail);
      false -> List
    end.
    %% END

dropwhile_test() ->
    F = fun(Val) -> Val =:= 32 end,
    ?assertEqual("hello", dropwhile(F, "   hello")),
    ?assertEqual([], dropwhile(F, [])),
    ?assertEqual([1,2,3], dropwhile(F, [1,2,3])),
    ?assertEqual([3,4], dropwhile(F, [32,3,4])),
    ?assertEqual([3,4], dropwhile(F, [32,32,3,4])),
    ?assertEqual([3,32,4,32], dropwhile(F, [32,32,32,32,32,32,3,32,4,32])),
    ok.


%% implement lists:takewhile/2
%% http://www.erlang.org/doc/man/lists.html#takewhile-2
takewhile(_, []) -> [];
  
takewhile(Pred, List) ->
    %% BEGIN
    [Head|Tail] = List,
    case Pred(Head) of
      true -> [Head|takewhile(Pred, Tail)];
      false -> []
    end.
    %% END

takewhile_test() ->
    F = fun(Val) -> Val =:= 32 end,
    ?assertEqual("   ", takewhile(F, "   hello")),
    ?assertEqual([], takewhile(F, [])),
    ?assertEqual([], takewhile(F, [1,2,3])),
    ?assertEqual([32], takewhile(F, [32,3,4])),
    ?assertEqual([32,32], takewhile(F, [32,32,3,4])),
    ?assertEqual([32,32,32,32,32,32], takewhile(F, [32,32,32,32,32,32,3,32,4,32])),
    ok.
