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
    seasons = [(1, "2019-2020"), (2, "2020-2021"), (3, "2021-2022"), 
              (4, "2022-2023"), (5, "2023-2024")]
    
    print("\nChecking seasons completion status:")
    for season_id, season_name in seasons:
        # Count how many teams have data for this season
        cur.execute("""
            SELECT COUNT(DISTINCT team_id) FROM ThreePointStats
            WHERE season_id = ?
        """, (season_id,))
        count = cur.fetchone()[0]
        print(f"  Season {season_name} (ID: {season_id}): {count}/30 teams")
        
        # If this season isn't complete, use it
        if count < 30:
            print(f"  → Selected for processing: {season_name}")
            return season_name, season_id, count
    
    # All seasons complete
    print("  All seasons complete!")
    return None, None, 0

def get_teams_to_process(cur, season_id, limit=15):
    """Get teams that don't have 3PT data yet for the specified season"""
    # Get teams that already have data for this season
    cur.execute("""
        SELECT team_id FROM ThreePointStats 
        WHERE season_id = ?
    """, (season_id,))
    existing_team_ids = [row[0] for row in cur.fetchall()]
    
    # Get teams that don't have data yet
    placeholders = ','.join(['?'] * len(existing_team_ids)) if existing_team_ids else "0"
    query = f"""
        SELECT t.team_id, t.team_name, t.team_abbreviation
        FROM Teams t
        WHERE t.team_id NOT IN ({placeholders})
        ORDER BY t.team_id
        LIMIT ?
    """
    
    # If there are existing teams, include them in the query
    params = existing_team_ids + [limit] if existing_team_ids else [limit]
    cur.execute(query, params)
    
    return cur.fetchall()

def three_point_data(cur, conn, season, season_id, limit=15):
    """Get 3-point shooting percentage data for a specific season"""
    # Get teams that need processing
    teams_to_process = get_teams_to_process(cur, season_id, limit)
    
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
            games_played = int(cols[1].text) if cols[1].text else 0
            
            # Check if this team-season combination already exists
            cur.execute("""
                SELECT COUNT(*) FROM ThreePointStats 
                WHERE team_id = ? AND season_id = ?
            """, (team_id, season_id))
            exists = cur.fetchone()[0] > 0
            
            if not exists:
                # Insert 3-point data into consolidated table
                cur.execute('''
                    INSERT INTO ThreePointStats 
                    (team_id, season_id, three_pt_percentage, three_pt_made, three_pt_attempts, games_played)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (team_id, season_id, three_pt_percentage, three_pt_made, three_pt_attempts, games_played))
                
                counter += 1
                print(f"Added 3PT stats for {team_name} ({season})")
            else:
                print(f"Team {team_name} already has data for season {season}, skipping")
                
        except (ValueError, IndexError) as e:
            print(f"Error processing 3PT stats for {team_name}: {e}")
    
    conn.commit()
    return counter

def main():
    conn, cur = get_connection()
    
    # Get the current season to process
    current_season, season_id, completed_count = get_current_season(cur)
    
    if not current_season:
        print("All seasons are complete! No more 3PT data to collect.")
        conn.close()
        return
        
    print(f"Processing season: {current_season} ({completed_count}/30 teams complete)")
    
    # Process a batch of teams (max 15) for the current season
    count = three_point_data(cur, conn, current_season, season_id, limit=15)
    print(f"Added {count} 3PT records for season {current_season}")
    
    # Show overall progress
    print("\nProgress Report:")
    seasons = [(1, "2019-2020"), (2, "2020-2021"), (3, "2021-2022"), 
              (4, "2022-2023"), (5, "2023-2024")]
    
    for season_id, season_name in seasons:
        cur.execute("SELECT COUNT(DISTINCT team_id) FROM ThreePointStats WHERE season_id = ?", (season_id,))
        count = cur.fetchone()[0]
        print(f"Season {season_name}: {count}/30 teams have 3PT data")
    
    conn.close()

if __name__ == "__main__":
    main()