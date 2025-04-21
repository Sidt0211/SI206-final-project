import time
import pandas as pd
import sqlite3
import os
from nba_api.stats.endpoints import teamyearbyyearstats
from nba_api.stats.static import teams

def get_connection(db_name="nba-stats.db"):
    """Connect to the database"""
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    return conn, conn.cursor()



def get_team_mappings():
    """Get mappings between NBA API team IDs and our database team IDs"""
    # Get all NBA teams
    nba_teams = teams.get_teams()
    
    # Create mappings
    nba_id_to_abbr = {team['id']: team['abbreviation'] for team in nba_teams}
    return nba_id_to_abbr

def get_teams_to_process(cur, season_db, limit=15):
    """Get teams that don't have defensive stats yet for the specified season"""
    cur.execute(f"""
        SELECT t.team_id, t.team_name, t.team_abbreviation 
        FROM Teams t 
        LEFT JOIN DefensiveStats_{season_db} ds ON t.team_id = ds.team_id
        WHERE ds.team_id IS NULL
        ORDER BY t.team_id
        LIMIT {limit}
    """)
    return cur.fetchall()

def process_team_defensive_stats(team_nba_id, season):
    """Process defensive stats for a specific team and season"""
    # Convert season format (2019_2020 → 2019-20)
    season_api = f"{season.split('_')[0][:4]}-{season.split('_')[1][-2:]}"
    
    try:
        # Call the TeamYearByYearStats endpoint
        team_stats = teamyearbyyearstats.TeamYearByYearStats(
            league_id='00',
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            team_id=team_nba_id
        )
        
        # Get stats as DataFrame
        df = team_stats.get_data_frames()[0]
        
        # Filter for the specific season
        season_stats = df[df['YEAR'] == season_api]
        
        if len(season_stats) == 0:
            print(f"No data found for team {team_nba_id} in season {season_api}")
            return None
        
        # Extract defensive stats
        defensive_stats = {
            'games_played': int(season_stats['GP'].values[0]),
            'defensive_rebounds': float(season_stats['DREB'].values[0]),
            'steals': float(season_stats['STL'].values[0]),
            'blocks': float(season_stats['BLK'].values[0]),
            'personal_fouls': float(season_stats['PF'].values[0])
        }
        
        return defensive_stats
    
    except Exception as e:
        print(f"Error fetching data for team {team_nba_id} in season {season_api}: {e}")
        return None

def process_defensive_stats_batch(cur, conn, season_db, nba_id_mapping, limit=15):
    """Process defensive stats for a batch of teams"""
    # Get teams that need processing
    teams_to_process = get_teams_to_process(cur, season_db, limit)
    
    if not teams_to_process:
        print(f"All teams already have defensive stats for season {season_db}")
        return 0
    
    # Get mapping from NBA abbreviation to our team_id
    abbr_to_our_id = {team[2]: team[0] for team in teams_to_process}
    
    # Get mapping from our team abbreviation to NBA team ID
    nba_teams = teams.get_teams()
    abbr_to_nba_id = {team['abbreviation']: team['id'] for team in nba_teams}
    
    counter = 0
    for our_team_abbr, our_team_id in abbr_to_our_id.items():
        # Find corresponding NBA team ID
        if our_team_abbr in abbr_to_nba_id:
            nba_team_id = abbr_to_nba_id[our_team_abbr]
            
            # Get defensive stats for this team
            defensive_stats = process_team_defensive_stats(nba_team_id, season_db)
            
            if defensive_stats:
                # Insert into database
                try:
                    cur.execute(f'''
                        INSERT OR REPLACE INTO DefensiveStats_{season_db} 
                        (team_id, defensive_rebounds, steals, blocks, personal_fouls, games_played)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        our_team_id, 
                        defensive_stats['defensive_rebounds'], 
                        defensive_stats['steals'], 
                        defensive_stats['blocks'], 
                        defensive_stats['personal_fouls'], 
                        defensive_stats['games_played']
                    ))
                    
                    counter += 1
                    print(f"Added defensive stats for {our_team_abbr} ({season_db.replace('_', '-')})")
                    
                except sqlite3.Error as e:
                    print(f"Database error for {our_team_abbr}: {e}")
            
            # Sleep to avoid rate limiting
            time.sleep(5)
    
    conn.commit()
    return counter

def main():
    # Connect to database
    conn, cur = get_connection()
    
    
    # Get NBA team mappings
    nba_id_mapping = get_team_mappings()
    
    # Get current season to process
    seasons = ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]
    
    for season in seasons:
        # Check if this season needs processing
        cur.execute(f"SELECT COUNT(*) FROM DefensiveStats_{season}")
        count = cur.fetchone()[0]
        
        if count < 30:
            print(f"Processing season {season.replace('_', '-')} ({count}/30 teams complete)")
            
            # Process up to 15 teams for this season (to stay under 25 item limit)
            added = process_defensive_stats_batch(cur, conn, season, nba_id_mapping, limit=15)
            print(f"Added defensive stats for {added} teams in season {season.replace('_', '-')}")
            
            # Stop after processing one incomplete season
            break
    
    # Show overall progress
    print("\nDefensive Stats Progress Report:")
    for season in seasons:
        cur.execute(f"SELECT COUNT(*) FROM DefensiveStats_{season}")
        count = cur.fetchone()[0]
        print(f"Season {season.replace('_', '-')}: {count}/30 teams have defensive stats")
    
    conn.close()

if __name__ == "__main__":
    main()