import matplotlib
import os
import re
import requests 
import unittest
import sqlite3 
import json 
import time
'''
Outline:
1. Grab data of all active teams
2. Create a function to calculate PER
3. Loop through the list of each team and each players to assign a PER 
4. Create a table using SQL (id, name, alias, active players list, win rate, highest PER)
5. Find the highest 
'''
API_Key = "iUgGkgFtewzhUNggd2vczYVa7hawxWVdXb2QLpz4"

years = [2024,2023,2022,2021,2020]

nba_aliases = [
    "ATL",  # Atlanta Hawks
    "BOS",  # Boston Celtics
    "BKN",  # Brooklyn Nets
    "CHA",  # Charlotte Hornets
    "CHI",  # Chicago Bulls
    "CLE",  # Cleveland Cavaliers
    "DAL",  # Dallas Mavericks
    "DEN",  # Denver Nuggets
    "DET",  # Detroit Pistons
    "GSW",  # Golden State Warriors
    "HOU",  # Houston Rockets
    "IND",  # Indiana Pacers
    "LAC",  # Los Angeles Clippers
    "LAL",  # Los Angeles Lakers
    "MEM",  # Memphis Grizzlies
    "MIA",  # Miami Heat
    "MIL",  # Milwaukee Bucks
    "MIN",  # Minnesota Timberwolves
    "NOP",  # New Orleans Pelicans
    "NYK",  # New York Knicks
    "OKC",  # Oklahoma City Thunder
    "ORL",  # Orlando Magic
    "PHI",  # Philadelphia 76ers
    "PHX",  # Phoenix Suns
    "POR",  # Portland Trail Blazers
    "SAC",  # Sacramento Kings
    "SAS",  # San Antonio Spurs
    "TOR",  # Toronto Raptors
    "UTA",  # Utah Jazz
    "WAS"   # Washington Wizards
]


def get_team_id() -> dict : 

    team_url = f"https://api.sportradar.com/nba/trial/v8/en/league/teams.json?api_key={API_Key}"
    headers = {"accept": "application/json"}
    response = requests.get(team_url, headers=headers)
    data = json.loads(response.text)
    team_id_map = {team["alias"]: team["id"] for team in data["teams"] if team.get("alias") in nba_aliases}
    return team_id_map

def get_players_for_teams() -> dict:
    """
    Retrieve a dictionary mapping team alias to a dictionary with team id and active player roster.
    The roster is a dictionary mapping each player's full name to their id.
    """
    teams = get_team_id()  # Mapping alias -> team id
    teams_with_roster = {}

    for alias, team_id in teams.items():
        # Build the team profile URL for active players; note the endpoint may vary.
        profile_url = f"https://api.sportradar.com/nba/trial/v8/en/teams/{team_id}/profile.json?api_key={API_Key}"
        headers = {"accept": "application/json"}
        response = requests.get(profile_url, headers=headers)
        
        # If the API call is successful, parse the roster.
        if response.status_code == 200:
            data = response.json()
            # Assume active players are under "players"; adjust if necessary.
            roster_list = data.get("players", [])
            # Build a dictionary of player full names mapped to their ids.
            roster = {player["full_name"]: player["id"] for player in roster_list if "full_name" in player}
        else:
            roster = {}
            print(f"Error retrieving profile for team {alias} (id: {team_id}). Status code: {response.status_code}")

        teams_with_roster[alias] = {"id": team_id, "roster": roster}

        time.sleep(5)

    return teams_with_roster

def highest_perfomance(team_with_roster,year): 
    highest_per = -9999
    highest_name = ""
    for team_info in team_with_roster.values():
        roster = team_info.get("roster", {})
        for player_id in roster.values():
            player_url = f"https://api.sportradar.com/nba/trial/v8/en/players/{player_id}/profile.json?api_key={API_Key}"
            headers = {"accept": "application/json"}
            response = requests.get(player_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                for season in data.get("seasons", []):
                     if season.get("year") == year and season.get("type") == "REG":
                        team_data = season["teams"][0]
                        average_efficiency = team_data.get("average", {}).get("efficiency")
                        if average_efficiency > highest_per:
                            highest_per = average_efficiency
                            highest_name = data["full_name"]
    return (highest_name,highest_per)
                
def main():
    print(get_players_for_teams())


if __name__ == '__main__':
    main()