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
    # Check all seasons in order
    seasons = ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]
    
    for season in seasons:
        # Count how many teams have data for this season
        cur.execute(f"SELECT COUNT(*) FROM WinStats_{season}")
        count = cur.fetchone()[0]
        
        # If this season isn't complete, use it
        if count < 30:
            return season.replace("_", "-"), count
    
    # All seasons complete
    return None, 0

def get_teams_to_process(cur, season_db, limit=15):
    """Get teams that don't have win stats yet for the specified season"""
    cur.execute(f"""
        SELECT t.team_id, t.team_name 
        FROM Teams t 
        LEFT JOIN WinStats_{season_db} s ON t.team_id = s.team_id
        WHERE s.team_id IS NULL
        ORDER BY t.team_id
        LIMIT {limit}
    """)
    return cur.fetchall()

def get_win_stats_for_season(cur, conn, season, limit=15):
    """Get win percentage data for a specific season"""
    # Convert season format to database table format
    db_season = season.replace("-", "_")
    
    # Get teams that need processing
    teams_to_process = get_teams_to_process(cur, db_season, limit)
    
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
                    # Insert win stats into season-specific table
                    cur.execute(f'''
                        INSERT OR REPLACE INTO WinStats_{db_season} 
                        (team_id, win_percentage, wins, losses)
                        VALUES (?, ?, ?, ?)
                    ''', (team_id, win_pct, wins, losses))
                    
                    teams_processed += 1
                    print(f"Added win stats for {team_name} ({season})")
            except (ValueError, IndexError) as e:
                print(f"Error processing win stats for {team_name}: {e}")
    
    conn.commit()
    return teams_processed

def main():
    conn, cur = get_connection()
    
    # Get the current season to process
    current_season, completed_count = get_current_season(cur)
    
    if not current_season:
        print("All seasons are complete! No more win stats data to collect.")
        conn.close()
        return
        
    print(f"Processing season: {current_season} ({completed_count}/30 teams complete)")
    
    # Process a batch of teams (max 15) for the current season
    count = get_win_stats_for_season(cur, conn, current_season, limit=15)
    print(f"Added {count} win stats records for season {current_season}")
    
    # Show overall progress
    print("\nProgress Report:")
    for season in ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]:
        cur.execute(f"SELECT COUNT(*) FROM WinStats_{season}")
        count = cur.fetchone()[0]
        print(f"Season {season.replace('_', '-')}: {count}/30 teams have win stats data")
    
    conn.close()

if __name__ == "__main__":
    main()