import requests
import sqlite3
import time
import os

def get_team_id_by_name(conn, team_name):
    """
    Get team_id from the Teams table based on team name.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT team_id FROM Teams WHERE team_name LIKE ?", ('%' + team_name + '%',))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

def fetch_defensive_stats(limit=25):
    """
    Fetch defensive rating and win percentage data from the Free NBA API.
    Stores data in the database with a limit of specified entries per run.
    
    Note: This API requires a key from RapidAPI.
    """
    # Connect to database
    conn = sqlite3.connect('nba_stats.db')
    cursor = conn.cursor()
    
    api_key = "e73db0a067mshc69080694f7b94bp1a8ebbjsn0b231efe0a7a"  # Your API key
    
    # Check if the DefensiveStats table exists, create it if it doesn't
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DefensiveStats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER,
        season TEXT,
        defensive_rating REAL,
        win_percentage REAL,
        FOREIGN KEY (team_id) REFERENCES Teams(team_id),
        UNIQUE(team_id, season)
    )
    ''')
    conn.commit()
    
    # Check which team-season combinations we already have
    cursor.execute("SELECT team_id, season FROM DefensiveStats")
    existing_data = set([(row[0], row[1]) for row in cursor.fetchall()])
    
    # Seasons to fetch (2019-2024)
    seasons = {
        "2019-20": "2019",
        "2020-21": "2020",
        "2021-22": "2021",
        "2022-23": "2022",
        "2023-24": "2023"
    }
    
    # Track how many items we've added in this run
    items_added = 0
    
    # API URLs
    base_url = "https://api-nba-v1.p.rapidapi.com"
    
    # Headers for API request
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "api-nba-v1.p.rapidapi.com"
    }
    
    # Fetch teams first
    teams_url = f"{base_url}/teams"
    teams_response = requests.get(teams_url, headers=headers)
    
    if teams_response.status_code != 200:
        print(f"Failed to fetch teams: {teams_response.text}")
        conn.close()
        return
    
    teams_data = teams_response.json()
    
    # Process each season
    for db_season, api_season in seasons.items():
        # Skip if we've reached the limit
        if items_added >= limit:
            break
        
        # Process each team
        for team in teams_data.get('response', []):
            # Skip if we've reached the limit
            if items_added >= limit:
                break
                
            team_name = team.get('name')
            team_id_in_api = team.get('id')
            
            if not team_name or not team_id_in_api:
                continue
                
            # Get team_id from our database
            team_id = get_team_id_by_name(conn, team_name)
            if not team_id:
                print(f"Could not find team_id for {team_name}")
                continue
                
            # Skip if we already have this team-season combination
            if (team_id, db_season) in existing_data:
                print(f"Data for {team_name} {db_season} already exists in database.")
                continue
                
            # Fetch team stats for the season
            stats_url = f"{base_url}/teams/statistics"
            querystring = {"id": str(team_id_in_api), "season": api_season}
            
            # Add delay to avoid rate limiting
            time.sleep(1)
            
            try:
                stats_response = requests.get(stats_url, headers=headers, params=querystring)
                
                if stats_response.status_code != 200:
                    print(f"Failed to fetch stats for {team_name} in {db_season}: {stats_response.text}")
                    continue
                    
                stats_data = stats_response.json()
                
                # Extract defensive rating (points allowed per game)
                team_stats = stats_data.get('response', [{}])[0]
                points_allowed = team_stats.get('points', {}).get('against', {}).get('average', None)
                
                if points_allowed is None:
                    print(f"No defensive stats found for {team_name} in {db_season}")
                    continue
                
                # Fetch team standings (for win percentage)
                standings_url = f"{base_url}/standings"
                standings_query = {"league": "standard", "season": api_season, "team": str(team_id_in_api)}
                
                # Add delay to avoid rate limiting
                time.sleep(1)
                
                standings_response = requests.get(standings_url, headers=headers, params=standings_query)
                
                if standings_response.status_code != 200:
                    print(f"Failed to fetch standings for {team_name} in {db_season}: {standings_response.text}")
                    continue
                    
                standings_data = standings_response.json()
                team_standings = standings_data.get('response', [{}])[0]
                
                # Calculate win percentage
                win_count = team_standings.get('win', {}).get('total', 0)
                loss_count = team_standings.get('loss', {}).get('total', 0)
                
                if win_count == 0 and loss_count == 0:
                    print(f"No standings data found for {team_name} in {db_season}")
                    continue
                    
                total_games = win_count + loss_count
                win_percentage = win_count / total_games if total_games > 0 else 0
                
                # Insert data into database
                try:
                    cursor.execute(
                        "INSERT INTO DefensiveStats (team_id, season, defensive_rating, win_percentage) VALUES (?, ?, ?, ?)",
                        (team_id, db_season, points_allowed, win_percentage)
                    )
                    conn.commit()
                    items_added += 1
                    print(f"Added {team_name} {db_season} data: DefRtg={points_allowed}, Win%={win_percentage}")
                except sqlite3.IntegrityError:
                    conn.rollback()
                    print(f"Data for {team_name} {db_season} already exists.")
                    
            except Exception as e:
                print(f"Error fetching data for {team_name} in {db_season}: {e}")
    
    print(f"Added {items_added} items in this run.")
    conn.close()

if __name__ == "__main__":
    fetch_defensive_stats(limit=25)
