import matplotlib
import os
import requests 
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

years = [2024,2023,2022]

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
            roster_list = data.get("players", [])
            # Build a dictionary of player full names mapped to their ids.
            roster = {player["full_name"]: player["id"] for player in roster_list if "full_name" in player}
        else:
            roster = {}
            print(f"Error retrieving profile for team {alias} (id: {team_id}). Status code: {response.status_code}")

        teams_with_roster[alias] = {"id": team_id, "roster": roster}
        
        time.sleep(7)

    return teams_with_roster

def collecting_perfomance(team_info: dict, year: int) -> dict:
    highest_per = float("-inf")
    highest_name = ""
    all_per = []

    roster = team_info.get("roster", {})
    for player_name, player_id in roster.items():
        player_url = f"https://api.sportradar.com/nba/trial/v8/en/players/{player_id}/profile.json?api_key={API_Key}"
        headers = {"accept": "application/json"}
        response = requests.get(player_url, headers=headers)
        
        if response.status_code == 429:
            print("Rate limit hit. Sleeping for 60 seconds...")
            time.sleep(60)
            continue
    
        if response.status_code == 200:
            data = response.json()
            for season in data.get("seasons", []):
                if season.get("year") == year and season.get("type") == "REG":
                    team_data = season["teams"][0]
                    efficiency = team_data.get("average", {}).get("efficiency")
                    if efficiency is not None:
                        all_per.append(efficiency)
                        if efficiency > highest_per:
                            highest_per = efficiency
                            highest_name = player_name
        else:
            print(f"Failed to get data for {player_name}, status: {response.status_code}")
        
        time.sleep(7)  

    team_avg_per = sum(all_per)/len(all_per) if all_per else None
    return {
        "Average PER": team_avg_per,
        "Highest PER": highest_per if highest_per != float("-inf") else None,
        "Name": highest_name,
        "Year": year
    }


def dict_for_database():
    complete_dict = {}
    #call get players for teams --> a nested dictionary 
    teams_dict = get_players_for_teams()
    #traverse the whole dictionary to add avg per, highest per, and name 
    for year in years:
        year_dict = {}
        for alias,team in teams_dict.items():
            per_stats = collecting_perfomance(team,year)
            name_only = list(team["roster"].keys())
            team_entry = {
                "Alias": alias,
                "ID": team["id"],
                "Roster": name_only
            }
            team_entry.update(per_stats)
            year_dict[alias] = team_entry
        complete_dict[year] = year_dict       

    #endpoint should be a dictionary with this structure: {"Alias": str, "ID": str, "Roster": list, "Average PER": int, "Highest PER": int, "Name":str}
    return complete_dict

def save_dict_to_json(data: dict, filename: str = "nba_team_per_data.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def create_database(teams_data: dict):
    '''
    retrieve the full dictionary with team alias, 
    name,roster (use .keys and join the list to make a string), 
    avg per, highest per and player w the highest per



    '''        
    pass    
def main():
    full_data = dict_for_database()
    save_dict_to_json(full_data)


if __name__ == '__main__':
    main()