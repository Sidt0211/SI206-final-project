import sqlite3
import json
import os
import requests
import time
from bs4 import BeautifulSoup

def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()

    # Drop existing tables
    cur.execute('DROP TABLE IF EXISTS ThreePointStats')
    cur.execute('DROP TABLE IF EXISTS Teams')
    cur.execute('DROP TABLE IF EXISTS WinStats')
    
    # Also drop the season-specific tables if they exist
    for season in ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]:
        cur.execute(f'DROP TABLE IF EXISTS ThreePointStats_{season}')
        cur.execute(f'DROP TABLE IF EXISTS WinStats_{season}')
    
    # Create Teams table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Teams (
            team_id INTEGER PRIMARY KEY,
            team_name TEXT UNIQUE,
            team_abbreviation TEXT
        )
    ''')
    
    # Create season-specific ThreePointStats tables
    for season in ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS ThreePointStats_{season} (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                team_id INTEGER,
                three_pt_percentage REAL,
                three_pt_made INTEGER,
                three_pt_attempts INTEGER,
                FOREIGN KEY (team_id) REFERENCES Teams(team_id),
                UNIQUE(team_id)
            )
        ''')

    # Create season-specific WinStats tables
    for season in ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS WinStats_{season} (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                team_id INTEGER,
                win_percentage REAL,
                wins INTEGER,
                losses INTEGER,
                FOREIGN KEY (team_id) REFERENCES Teams(team_id),
                UNIQUE(team_id)
            )
        ''')
    
    conn.commit()
    return cur, conn

def get_teams_data(cur, conn):
    teams = [
        {"name": "Atlanta Hawks", "abbreviation": "ATL"},
        {"name": "Boston Celtics", "abbreviation": "BOS"},
        {"name": "Brooklyn Nets", "abbreviation": "BRK"},
        {"name": "Charlotte Hornets", "abbreviation": "CHO"},
        {"name": "Chicago Bulls", "abbreviation": "CHI"},
        {"name": "Cleveland Cavaliers", "abbreviation": "CLE"},
        {"name": "Dallas Mavericks", "abbreviation": "DAL"},
        {"name": "Denver Nuggets", "abbreviation": "DEN"},
        {"name": "Detroit Pistons", "abbreviation": "DET"},
        {"name": "Golden State Warriors", "abbreviation": "GSW"},
        {"name": "Houston Rockets", "abbreviation": "HOU"},
        {"name": "Indiana Pacers", "abbreviation": "IND"},
        {"name": "Los Angeles Clippers", "abbreviation": "LAC"},
        {"name": "Los Angeles Lakers", "abbreviation": "LAL"},
        {"name": "Memphis Grizzlies", "abbreviation": "MEM"},
        {"name": "Miami Heat", "abbreviation": "MIA"},
        {"name": "Milwaukee Bucks", "abbreviation": "MIL"},
        {"name": "Minnesota Timberwolves", "abbreviation": "MIN"},
        {"name": "New Orleans Pelicans", "abbreviation": "NOP"},
        {"name": "New York Knicks", "abbreviation": "NYK"},
        {"name": "Oklahoma City Thunder", "abbreviation": "OKC"},
        {"name": "Orlando Magic", "abbreviation": "ORL"},
        {"name": "Philadelphia 76ers", "abbreviation": "PHI"},
        {"name": "Phoenix Suns", "abbreviation": "PHO"},
        {"name": "Portland Trail Blazers", "abbreviation": "POR"},
        {"name": "Sacramento Kings", "abbreviation": "SAC"},
        {"name": "San Antonio Spurs", "abbreviation": "SAS"},
        {"name": "Toronto Raptors", "abbreviation": "TOR"},
        {"name": "Utah Jazz", "abbreviation": "UTA"},
        {"name": "Washington Wizards", "abbreviation": "WAS"},
    ]

    counter = 1
    for team in teams:
        cur.execute('''
            INSERT OR IGNORE INTO Teams (team_id, team_name, team_abbreviation)
            VALUES (?, ?, ?)
        ''', (counter, team["name"], team["abbreviation"]))
        counter += 1
    conn.commit()
    return counter

def three_point_data(cur, conn, season):
    """Get 3-point shooting percentage data for a specific season"""
    # Convert season format to database table format (replace dash with underscore)
    db_season = season.replace("-", "_")
    
    # Season code format example: "2019-20" for 2019-2020 season
    season_code = f"{season.split('-')[0]}-{season.split('-')[1][-2:]}"
    url = f"https://www.basketball-reference.com/leagues/NBA_{season.split('-')[1]}.html"
    
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the team stats table - correct ID for team totals
    total_stats = soup.find('table', id='totals-team')
    
    if not total_stats:
        print(f"Could not find team stats for season {season}")
        return 0
    
    rows = total_stats.find_all('tr')[1:]  
    
    counter = 0
    for row in rows:
        if counter == 30:
            break

        cols = row.find_all('td')
        if not cols:
            continue
            
        team_name = cols[0].text.strip().replace("*", "")
        three_pt_made = int(cols[6].text) if cols[6].text else 0
        three_pt_attempts = int(cols[7].text) if cols[7].text else 0
        three_pt_percentage = float(cols[8].text) if cols[8].text else 0

        # print(f"Processing {team_name} ({three_pt_made}) made, {three_pt_attempts} attempts, {three_pt_percentage}%)")
        
        # Get team_id from the database
        cur.execute("SELECT team_id FROM Teams WHERE team_name = ?", (team_name,))
        result = cur.fetchone()
        
        if result:
            team_id = result[0]
            # Insert 3-point data into season-specific table
            cur.execute(f'''
                INSERT OR IGNORE INTO ThreePointStats_{db_season} 
                (team_id, three_pt_percentage, three_pt_made, three_pt_attempts)
                VALUES (?, ?, ?, ?)
            ''', (team_id, three_pt_percentage, three_pt_made, three_pt_attempts))
            
            if cur.rowcount > 0:
                counter += 1
                print(f"Added 3PT stats for {team_name} ({season})")
    
    conn.commit()
    return counter

def get_win_stats_for_season(cur, conn, season):
    """Get win percentage data for a specific season"""
    # Convert season format to database table format (replace dash with underscore)
    db_season = season.replace("-", "_")
    
    season_year = season.split('-')[1]
    url = f"https://www.basketball-reference.com/leagues/NBA_{season_year}.html"
    
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
                        INSERT OR IGNORE INTO WinStats_{db_season} 
                        (team_id, win_percentage, wins, losses)
                        VALUES (?, ?, ?, ?)
                    ''', (team_id, win_pct, wins, losses))
                    
                    if cur.rowcount > 0:
                        teams_processed += 1
                        print(f"Added win stats for {team_name} ({season})")
            except (ValueError, IndexError) as e:
                print(f"Error processing win stats for {team_name}: {e}")
    
    conn.commit()
    return teams_processed

def main():
    cur, conn = set_up_database("nba-stats.db")
    
    # 1. First insert teams data
    get_teams_data(cur, conn)
    
    # 2. Get team stats for each season (limited to 25 entries per run)
    seasons = [
        "2019-2020", 
        "2020-2021", 
        "2021-2022", 
        "2022-2023", 
        "2023-2024"
    ]
    
    for season in seasons:
        print(f"\nProcessing season: {season}")
        
        three_point_data(cur, conn, season)
        time.sleep(5)
        
        get_win_stats_for_season(cur, conn, season)
        time.sleep(5)
    
    # "test cases or just to see how much data we got"
    total_3pt_stats = 0
    total_win_stats = 0
    
    # Count records in each season-specific table
    for season in ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]:
        cur.execute(f"SELECT COUNT(*) FROM ThreePointStats_{season}")
        season_3pt_count = cur.fetchone()[0]
        total_3pt_stats += season_3pt_count
        
        cur.execute(f"SELECT COUNT(*) FROM WinStats_{season}")
        season_win_count = cur.fetchone()[0]
        total_win_stats += season_win_count
        
        print(f"\nSeason {season.replace('_', '-')} Stats:")
        print(f"  3-point stats records: {season_3pt_count}/30")
        print(f"  Win stats records: {season_win_count}/30")
    
    print("\nData Collection Summary:")
    print(f"Total 3-point stats records: {total_3pt_stats}/150 (target: 30 teams × 5 seasons)")
    print(f"Total win stats records: {total_win_stats}/150 (target: 30 teams × 5 seasons)")
    
    conn.close()

if __name__ == "__main__":
    main()