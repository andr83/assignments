-module(champ_stat).

-export([get_stat/1]).
-include_lib("eunit/include/eunit.hrl").

-import(champ, [sample_champ/0]).


get_stat(Champ) ->
    %% BEGIN
    F = fun({team, _, Players}, {NumTeams, NumPlayers, TotalAge, TotalRating}) ->
          {NumTeamPlayers, TotalTeamAge, TotalTeamRating} = get_team_stat(Players),
          {NumTeams + 1, NumPlayers + NumTeamPlayers, TotalAge + TotalTeamAge, TotalRating + TotalTeamRating}
        end,                                                                                   
    {NumTeams, NumPlayers, TotalAge, TotalRating} = lists:foldl(F, {0, 0, 0, 0}, Champ),
    {NumTeams, NumPlayers, TotalAge / NumPlayers, TotalRating / NumPlayers}.
    %% END

get_team_stat(Team) ->
  F = fun({player, _, Age, Rating, _}, {NumTeamPlayers, TotalTeamAge, TotalTeamRating}) ->
        {NumTeamPlayers + 1, TotalTeamAge + Age, TotalTeamRating + Rating}
      end,                                                                                    
  lists:foldl(F, {0, 0, 0}, Team).                                                                                          

get_stat_test() ->
    ?assertEqual({5,40,24.85,242.8}, get_stat(champ:sample_champ())),
    ok.

