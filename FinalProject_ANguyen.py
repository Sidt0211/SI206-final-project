import matplotlib
import os
import requests 
import sqlite3 
import json 
import time

API_Key1 = "niICbC63cETOJJlTriWtlrrHendyrbZXUEu8Ax1i"

API_Key2 = "udrGJy8IqXMGz5Hl5WAkxTAYzbNK82GKBykSRrpO"


years = 2024

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


def get_team_id(teams_to_process) -> dict : 

    team_url = f"https://api.sportradar.com/nba/trial/v8/en/league/teams.json?api_key={API_Key2}"
    headers = {"accept": "application/json"}
    response = requests.get(team_url, headers=headers)
    data = json.loads(response.text)
    team_id_map = {team["alias"]: team["id"] for team in data["teams"] if team.get("alias") in teams_to_process}
    
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, "teams_info.json")
    with open(file_path, "w",encoding="utf-8-sig") as teams_file:
        json.dump(data, teams_file, indent=4)

    return team_id_map


def get_players_for_teams(teams_to_process) -> dict:
    """
    Retrieve a dictionary mapping team alias to a dictionary with team id and active player roster.
    The roster is a dictionary mapping each player's full name to their id.
    """
    teams = get_team_id(teams_to_process)  # Mapping alias -> team id
    teams_with_roster = {}

    for alias, team_id in teams.items():
        # Build the team profile URL for active players; note the endpoint may vary.
        profile_url = f"https://api.sportradar.com/nba/trial/v8/en/teams/{team_id}/profile.json?api_key={API_Key1}"
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
        
        time.sleep(2)

    return teams_with_roster

def collecting_performance(team_info: dict, year: int) -> dict:
    """
    Collects performance data for players on a team and returns a dictionary
    with the 5 players who have the highest efficiency (PER).
    
    Args:
        team_info: Dictionary containing team id and roster information
        year: Year to collect data for
        
    Returns:
        Dictionary with top 5 players sorted by PER
    """
    # Dictionary to store player name and their PER
    player_per_dict = {}
    
    roster = team_info.get("roster", {})
    # Loop through each player in the roster
    for player_name, player_id in roster.items():
        player_url = f"https://api.sportradar.com/nba/trial/v8/en/players/{player_id}/profile.json?api_key={API_Key2}"
        headers = {"accept": "application/json"}
        response = requests.get(player_url, headers=headers)
        
        if response.status_code == 429:
            print("Rate limit hit. Sleeping for 60 seconds...")
            time.sleep(60)
            continue
    
        if response.status_code == 200:
            data = response.json()
            # Find data for the specified year and regular season
            for season in data.get("seasons", []):
                if season.get("year") == year and season.get("type") == "REG":
                    # Get the team data - assume first team in the list
                    if season.get("teams") and len(season["teams"]) > 0:
                        team_data = season["teams"][0]
                        efficiency = team_data.get("average", {}).get("efficiency")
                        if efficiency is not None:
                            # Store player name and their efficiency
                            player_per_dict[player_name] = efficiency
                            break
        else:
            print(f"Failed to get data for {player_name}, status: {response.status_code}")
        
        time.sleep(2)  # Respect API rate limits
    
    # Sort the players by PER in descending order and get the top 5
    top_players = {}
    counter = 1
    
    # Sort the dictionary items by PER value (descending)
    sorted_players = sorted(player_per_dict.items(), key=lambda x: x[1], reverse=True)
    
    # Take only the top 5 players (or fewer if there aren't 5 players with PER data)
    for name, per in sorted_players[:5]:
        top_players[f"Player_{counter}"] = {
            "name": name,
            "efficiency": per
        }
        counter += 1
    
    return top_players


def dict_for_database(teams_to_process):
    teams_dict = get_players_for_teams(teams_to_process)
    complete_dict = {}
    
    for alias, team in teams_dict.items():
        # Get top 5 players by PER
        top_players = collecting_performance(team, years)  # years is the single integer 2024
        
        # Create team entry with the top players as a dedicated field
        team_entry = {
            "Alias": alias,
            "ID": team["id"],
            "Roster": list(team["roster"].keys()),
            "TopPlayers": top_players  # Store top players in their own field
        }
        
        complete_dict[alias] = team_entry
        print(f"Processed {alias}")
    
    return complete_dict

def save_dict_to_json(data: dict, filename: str = "nba_team_per_data.json"):
    if os.path.exists(filename):

        with open(filename, "r", encoding="utf-8") as f:
            existing_data = json.load(f)

            existing_data.update(data)
            data_save = existing_data
    else:
        data_save = data
    
    with open(filename,'w',encoding = "utf-8") as f:
        json.dump(data_save,f,indent = 4)

def create_database():
    """
    Create the database and table structure only if it doesn't exist yet.
    """
    conn = sqlite3.connect("nba_efficiency.db")
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS top_players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_alias TEXT NOT NULL,
        player_name TEXT NOT NULL,
        efficiency REAL NOT NULL
    )
    ''')
    
    conn.commit()
    conn.close()
    print("Database and table created successfully.")

def insert_team_data(teams_data):
    """Insert team data into the database"""
    conn = sqlite3.connect("nba_efficiency.db")
    cursor = conn.cursor()
    
    # Get list of teams already in the database to avoid duplicates
    cursor.execute("SELECT DISTINCT team_alias FROM top_players")
    existing_teams = [team[0] for team in cursor.fetchall()]
    
    teams_inserted = 0
    
    # Insert data into the table
    for alias, team_data in teams_data.items():
        # Skip if team already exists in database
        if alias in existing_teams:
            print(f"Team {alias} already exists in database. Skipping.")
            continue
            
        # Insert top players with team alias
        if "TopPlayers" in team_data:
            for player_key in team_data["TopPlayers"]:
                player_data = team_data["TopPlayers"][player_key]
                cursor.execute(
                    "INSERT INTO top_players (team_alias, player_name, efficiency) VALUES (?, ?, ?)",
                    (alias, player_data["name"], player_data["efficiency"])
                )
            teams_inserted += 1
    
    conn.commit()
    conn.close()
    print(f"Inserted data for {teams_inserted} new teams.")

def get_processed_teams():
    """Get list of teams already processed in the database"""
    conn = sqlite3.connect("nba_efficiency.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT team_alias FROM top_players")
    processed_teams = [team[0] for team in cursor.fetchall()]
    
    conn.close()
    return processed_teams

def get_next_batch(batch_size=5):
    """Get the next batch of teams to process"""
    processed_teams = get_processed_teams()
    
    remaining_teams = []
    for team in nba_aliases: 
        if team not in processed_teams:
            remaining_teams.append(team) 
    
    # Return the next batch, or empty list if all done
    return remaining_teams[:batch_size]

def main():
    # Create the database structure if it doesn't exist
    create_database()
    
    # Get the next batch of teams to process
    teams_to_process = get_next_batch(5)
    
    if not teams_to_process:
        print("All teams have been processed!")
        return
        
    print(f"Processing teams: {', '.join(teams_to_process)}")
    
    # Process the batch of teams
    full_data = dict_for_database(teams_to_process)
    
    save_dict_to_json(full_data)
    insert_team_data(full_data)
    
    # Report remaining teams
    processed_teams = get_processed_teams()
    remaining = []
    for team in nba_aliases: 
        if team not in processed_teams:
            remaining.append(team) 
    print(f"Teams processed: {len(processed_teams)}/{len(nba_aliases)}")
    print(f"Teams remaining: {len(remaining)}")
    if remaining:
        print(f"Next batch will be: {', '.join(remaining[:5])}")

if __name__ == '__main__':
    main()