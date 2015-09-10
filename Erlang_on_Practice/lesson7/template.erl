-module(template).

-export([parse/2]).

parse(Str, Data) when is_binary(Str) ->
    %% BEGIN (write your solution here)
    M1 = binary:matches(Str, <<"{{">>),
    M2 = binary:matches(Str, <<"}}">>),
  
    F = fun({BeginPos, BeginLen}, {Idx, Res}) ->
          {EndPos, _} = lists:nth(Idx, M2),
          VarName = binary_part(Str, BeginPos + BeginLen, EndPos - BeginPos - BeginLen),          
          TplVarName = erlang:iolist_to_binary([<<"{{">>, VarName, <<"}}">>]),
          NewStr = case maps:find(VarName, Data) of
                       error ->
                            binary:replace(Res, TplVarName, <<"">>);                       
                       {ok, Value} ->                    
                       		binary:replace(Res, TplVarName, convert_to_binary(Value))
                   end,
          {Idx + 1, NewStr}
        end,  
    {_, NewStr} = lists:foldl(F, {1, Str}, M1),
    NewStr.
    %% END

convert_to_binary(Value) when is_binary(Value) -> Value;
convert_to_binary(Value) when is_integer(Value) -> list_to_binary(integer_to_list(Value));
convert_to_binary(Value) when is_list(Value) -> unicode:characters_to_binary(Value).



