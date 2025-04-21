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
    """Determine which season to process next for win stats data"""
    seasons = [(1, "2019-2020"), (2, "2020-2021"), (3, "2021-2022"), 
              (4, "2022-2023"), (5, "2023-2024")]
    
    print("\nChecking seasons completion status:")
    for season_id, season_name in seasons:
        # Count how many teams have data for this season
        cur.execute("""
            SELECT COUNT(DISTINCT team_id) FROM WinStats
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
    """Get teams that don't have win stats yet for the specified season"""
    # Get teams that already have data for this season
    cur.execute("""
        SELECT team_id FROM WinStats 
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

def get_win_stats_for_season(cur, conn, season, season_id, limit=15):
    """Get win percentage data for a specific season"""
    # Get teams that need processing
    teams_to_process = get_teams_to_process(cur, season_id, limit)
    
    if not teams_to_process:
        print(f"All teams already have win stats for season {season}")
        return 0
    
    team_ids_to_process = [team[0] for team in teams_to_process]
    team_names_to_process = [team[1] for team in teams_to_process]
    print(f"Processing win stats for up to {len(teams_to_process)} teams in season {season}")
    
    # Use the second year of the season for the URL
    season_year = season.split('-')[1]
    url = f"https://www.basketball-reference.com/leagues/NBA_{season_year}.html"
    
    print(f"Fetching data from: {url}")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    teams_processed = 0
    conferences = ['E', 'W']
    for conference in conferences:
        standings_table = soup.find('table', id=f'confs_standings_{conference}')
        if not standings_table:
            print(f"Could not find {conference} conference standings for season {season}")
            continue
        
        rows = standings_table.find_all('tr')[1:]
        
        for row in rows:
            cells = row.find_all(['th', 'td'])
            if len(cells) < 4:
                continue
                
            team_cell = cells[0]
            if team_cell.find('a'):
                team_name = team_cell.find('a').text.strip()
            else:
                team_name = team_cell.text.strip()
                
            # Skip if this team is not in our batch to process
            if team_name not in team_names_to_process:
                continue
                
            try:
                wins = int(cells[1].text) if cells[1].text else 0
                losses = int(cells[2].text) if cells[2].text else 0
                win_pct = float(cells[3].text) if cells[3].text else 0.0
                
                cur.execute("SELECT team_id FROM Teams WHERE team_name = ?", (team_name,))
                result = cur.fetchone()
                
                if result:
                    team_id = result[0]
                    
                    # Check if this team-season combination already exists
                    cur.execute("""
                        SELECT COUNT(*) FROM WinStats 
                        WHERE team_id = ? AND season_id = ?
                    """, (team_id, season_id))
                    exists = cur.fetchone()[0] > 0
                    
                    if not exists:
                        # Insert win stats into consolidated table
                        cur.execute('''
                            INSERT INTO WinStats 
                            (team_id, season_id, win_percentage, wins, losses)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (team_id, season_id, win_pct, wins, losses))
                        
                        teams_processed += 1
                        print(f"Added win stats for {team_name} ({season})")
                    else:
                        print(f"Team {team_name} already has data for season {season}, skipping")
            except (ValueError, IndexError) as e:
                print(f"Error processing win stats for {team_name}: {e}")
    
    conn.commit()
    return teams_processed

def main():
    conn, cur = get_connection()
    
    # Get the current season to process
    current_season, season_id, completed_count = get_current_season(cur)
    
    if not current_season:
        print("All seasons are complete! No more win stats data to collect.")
        conn.close()
        return
        
    print(f"Processing season: {current_season} ({completed_count}/30 teams complete)")
    
    # Process a batch of teams (max 15) for the current season
    count = get_win_stats_for_season(cur, conn, current_season, season_id, limit=15)
    print(f"Added {count} win stats records for season {current_season}")
    
    # Show overall progress
    print("\nProgress Report:")
    seasons = [(1, "2019-2020"), (2, "2020-2021"), (3, "2021-2022"), 
              (4, "2022-2023"), (5, "2023-2024")]
    
    for season_id, season_name in seasons:
        cur.execute("SELECT COUNT(DISTINCT team_id) FROM WinStats WHERE season_id = ?", (season_id,))
        count = cur.fetchone()[0]
        print(f"Season {season_name}: {count}/30 teams have win stats data")
    
    conn.close()

if __name__ == "__main__":
    main()