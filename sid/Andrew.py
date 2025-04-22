import os
import requests 
import sqlite3 
import json 
import time

API_Key1 = "vSAAKizKQlfaqbsW7ZaPInM5dwhwap32DARv24jy"
API_Key2 = "yZ6bmvDuyiUyEJ2J6TycitB2z6AF8PwUX9KptsZv"
API_KEY3 = "ovBA9X4cWiwFzHueVPLSXbrn9eny51b09g9jR2rU"

years = 2024

def get_connection():
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/nba-stats.db")
    return conn, conn.cursor()

def get_teams_from_db():
    """Get all teams from the Teams table in the database"""
    conn, cur = get_connection()
    
    cur.execute("SELECT team_id, team_name, team_abbreviation FROM Teams")
    teams = cur.fetchall()
    
    conn.close()
    return teams

def get_team_api_id(team_abbreviation):
    """Get the Sports Radar API team ID for a given team abbreviation"""
    team_url = f"https://api.sportradar.com/nba/trial/v8/en/league/teams.json?api_key={API_KEY3}"
    headers = {"accept": "application/json"}
    response = requests.get(team_url, headers=headers)

    data = json.loads(response.text)
    
    for team in data["teams"]:
        if team.get("alias") == team_abbreviation:
            return team["id"]
    
    return None

def get_players_for_team(team_id, team_abbreviation):
    """
    Retrieve player roster for a team using the Sports Radar API
    """
    api_team_id = get_team_api_id(team_abbreviation)
    if not api_team_id:
        print(f"Could not find API ID for team {team_abbreviation}")
        return {"team_id": team_id, "roster": {}}
    
    profile_url = f"https://api.sportradar.com/nba/trial/v8/en/teams/{api_team_id}/profile.json?api_key={API_KEY3}"
    headers = {"accept": "application/json"}
    response = requests.get(profile_url, headers=headers)
    
    # API call is successful --> parse the roster.
    if response.status_code == 200:
        data = response.json()
        roster_list = data.get("players", [])
        roster = {player["full_name"]: player["id"] for player in roster_list if "full_name" in player}
    else:
        roster = {}
        print(f"Error retrieving profile for team {team_abbreviation}. Status code: {response.status_code}")

    time.sleep(2)  # Respect API rate limits
    return {"team_id": team_id, "roster": roster}

def collecting_performance(team_info, year):
    """Collect player efficiency data for a team"""
    player_per_dict = {}
    
    roster = team_info.get("roster", {})
    for player_name, player_id in roster.items():
        player_url = f"https://api.sportradar.com/nba/trial/v8/en/players/{player_id}/profile.json?api_key={API_KEY3}"
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
                    if season.get("teams") and len(season["teams"]) > 0:
                        team_data = season["teams"][0]
                        efficiency = team_data.get("average", {}).get("efficiency")
                        if efficiency is not None:
                            player_per_dict[player_name] = efficiency
                            break
        else:
            print(f"Failed to get data for {player_name}, status: {response.status_code}")
        
        time.sleep(2)  # Respect API rate limits
    
    # Sort players by efficiency and get top 5
    top_players = []
    sorted_players = sorted(player_per_dict.items(), key=lambda x: x[1], reverse=True)
    
    for name, per in sorted_players[:5]:
        top_players.append({
            "name": name,
            "efficiency": per
        })
    
    return top_players

def create_efficiency_table():
    """Create the PlayerEfficiency table if it doesn't exist"""
    conn, cur = get_connection()
    
    cur.execute('''
    CREATE TABLE IF NOT EXISTS PlayerEfficiency (
        player_id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER,
        player_name TEXT,
        efficiency REAL,
        FOREIGN KEY (team_id) REFERENCES Teams(team_id)
    )
    ''')
    
    conn.commit()
    conn.close()
    print("PlayerEfficiency table created successfully.")

def get_processed_teams():
    """Get list of team_ids already processed in the database"""
    conn, cur = get_connection()
    
    cur.execute("""
        SELECT DISTINCT team_id FROM PlayerEfficiency
    """)
    
    processed_teams = [team[0] for team in cur.fetchall()]
    
    conn.close()
    return processed_teams

def get_next_batch(batch_size=5):
    """Get next batch of teams to process"""
    processed_team_ids = get_processed_teams()
    
    conn, cur = get_connection()
    
    # Get teams that don't have data yet
    placeholders = ','.join(['?'] * len(processed_team_ids)) if processed_team_ids else "0"
    query = f"""
        SELECT team_id, team_name, team_abbreviation
        FROM Teams
        WHERE team_id NOT IN ({placeholders})
        ORDER BY team_id
        LIMIT ?
    """
    
    params = processed_team_ids + [batch_size] if processed_team_ids else [batch_size]
    cur.execute(query, params)
    
    teams_to_process = cur.fetchall()
    
    conn.close()
    return teams_to_process

def insert_efficiency_data(teams_data):
    """Insert player efficiency data into the database"""
    conn, cur = get_connection()
    
    teams_inserted = 0
    
    for team_info in teams_data:
        team_id = team_info["team_id"]
        top_players = team_info["top_players"]
        
        # Check if this team already exists
        cur.execute("""
            SELECT COUNT(*) FROM PlayerEfficiency 
            WHERE team_id = ?
        """, (team_id,))
        
        if cur.fetchone()[0] > 0:
            print(f"Team ID {team_id} already has data, skipping")
            continue
        
        # Insert player efficiency data
        for player in top_players:
            cur.execute('''
                INSERT INTO PlayerEfficiency 
                (team_id, player_name, efficiency)
                VALUES (?, ?, ?)
            ''', (team_id, player["name"], player["efficiency"]))
        
        teams_inserted += 1
    
    conn.commit()
    conn.close()
    print(f"Inserted efficiency data for {teams_inserted} teams")

def process_teams(teams):
    """Process a batch of teams and collect their efficiency data"""
    results = []
    
    for team_id, team_name, team_abbreviation in teams:
        print(f"Processing {team_name} ({team_abbreviation})")
        
        # Get team players
        team_info = get_players_for_team(team_id, team_abbreviation)
        
        # Get efficiency data for top players
        top_players = collecting_performance(team_info, years)
        
        results.append({
            "team_id": team_id,
            "top_players": top_players
        })
    
    return results

def main():
    
    # Create the efficiency table if it doesn't exist
    create_efficiency_table()
    
    # Get the next batch of teams to process
    teams_to_process = get_next_batch(5)
    
    if len(teams_to_process) == 0:
        print("All teams have been processed!")
        return
    
    print(f"Processing {len(teams_to_process)} teams")
    
    # Process the batch of teams
    processed_data = process_teams(teams_to_process)
    
    # Insert the data into the database
    insert_efficiency_data(processed_data)
    
    # Report progress
    conn, cur = get_connection()
    cur.execute("SELECT COUNT(*) FROM Teams")
    total_teams = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT team_id) FROM PlayerEfficiency")
    processed_count = cur.fetchone()[0]
    
    remaining = total_teams - processed_count
    print(f"Teams processed: {processed_count}/{total_teams}")
    print(f"Teams remaining: {remaining}")
    
    conn.close()

if __name__ == '__main__':
    main()