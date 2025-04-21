import sqlite3
import os
import requests
import time
from bs4 import BeautifulSoup

def get_connection():
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/nba-stats.db")
    return conn, conn.cursor()

def get_current_season(cur):
    """Determine which season to process next for 3PT data"""
    # Check all seasons in order
    seasons = ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]
    
    for season in seasons:
        # Count how many teams have data for this season
        cur.execute(f"SELECT COUNT(*) FROM ThreePointStats_{season}")
        count = cur.fetchone()[0]
        
        # If this season isn't complete, use it
        if count < 30:
            return season.replace("_", "-"), count
    
    # All seasons complete
    return None, 0

def get_teams_to_process(cur, season_db, limit=15):
    """Get teams that don't have 3PT data yet for the specified season"""
    cur.execute(f"""
        SELECT t.team_id, t.team_name 
        FROM Teams t 
        LEFT JOIN ThreePointStats_{season_db} s ON t.team_id = s.team_id
        WHERE s.team_id IS NULL
        ORDER BY t.team_id
        LIMIT {limit}
    """)
    return cur.fetchall()

def three_point_data(cur, conn, season, limit=15):
    """Get 3-point shooting percentage data for a specific season"""
    # Convert season format to database table format (replace dash with underscore)
    db_season = season.replace("-", "_")
    
    # Get teams that need processing
    teams_to_process = get_teams_to_process(cur, db_season, limit)
    
    if not teams_to_process:
        print(f"All teams already have 3PT data for season {season}")
        return 0
    
    team_ids_to_process = [team[0] for team in teams_to_process]
    print(f"Processing 3PT data for {len(teams_to_process)} teams in season {season}")
    
    # Use the second year of the season for the URL
    season_year = season.split('-')[1]
    url = f"https://www.basketball-reference.com/leagues/NBA_{season_year}.html"
    
    print(f"Fetching data from: {url}")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the team stats table
    total_stats = soup.find('table', id='totals-team')
    
    if not total_stats:
        print(f"Could not find team stats for season {season}")
        return 0
    
    rows = total_stats.find_all('tr')
    
    counter = 0
    for row in rows:
        cols = row.find_all('td')
        if not cols:
            continue
            
        team_name = cols[0].text.strip().replace("*", "")
        
        # Get team_id from the database
        cur.execute("SELECT team_id FROM Teams WHERE team_name = ?", (team_name,))
        result = cur.fetchone()
        
        if not result:
            continue
            
        team_id = result[0]
        
        # Skip if this team is not in our batch to process
        if team_id not in team_ids_to_process:
            continue
            
        try:
            three_pt_made = int(cols[6].text) if cols[6].text else 0
            three_pt_attempts = int(cols[7].text) if cols[7].text else 0
            three_pt_percentage = float(cols[8].text) if cols[8].text else 0
            games_played = int(cols[1].text) if cols[4].text else 0
            
            # Insert 3-point data into season-specific table
            cur.execute(f'''
                INSERT OR REPLACE INTO ThreePointStats_{db_season} 
                (team_id, three_pt_percentage, three_pt_made, three_pt_attempts, games_played)
                VALUES (?, ?, ?, ?, ?)
            ''', (team_id, three_pt_percentage, three_pt_made, three_pt_attempts, games_played))
            
            counter += 1
            print(f"Added 3PT stats for {team_name} ({season})")
        except (ValueError, IndexError) as e:
            print(f"Error processing 3PT stats for {team_name}: {e}")
    
    conn.commit()
    return counter

def main():
    conn, cur = get_connection()
    
    # Get the current season to process
    current_season, completed_count = get_current_season(cur)
    
    if not current_season:
        print("All seasons are complete! No more 3PT data to collect.")
        conn.close()
        return
        
    print(f"Processing season: {current_season} ({completed_count}/30 teams complete)")
    
    # Process a batch of teams (max 15) for the current season
    count = three_point_data(cur, conn, current_season, limit=15)
    print(f"Added {count} 3PT records for season {current_season}")
    
    # Show overall progress
    print("\nProgress Report:")
    for season in ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]:
        cur.execute(f"SELECT COUNT(*) FROM ThreePointStats_{season}")
        count = cur.fetchone()[0]
        print(f"Season {season.replace('_', '-')}: {count}/30 teams have 3PT data")
    
    conn.close()

if __name__ == "__main__":
    main()