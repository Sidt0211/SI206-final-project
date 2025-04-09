# sid
import sqlite3
import json
import os
import requests
from bs4 import BeautifulSoup

# def read_data_from_file(filename):
#     full_path = os.path.join(os.path.dirname(__file__), filename)
#     f = open(full_path)
#     file_data = f.read()
#     f.close()
#     json_data = json.loads(file_data)
#     return json_data


def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()

    cur.execute('DROP TABLE IF EXISTS ThreePointStats')
    cur.execute('DROP TABLE IF EXISTS Teams')
    
    # Create Teams table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Teams (
            team_id INTEGER PRIMARY KEY,
            team_name TEXT UNIQUE,
            team_abbreviation TEXT
        )
    ''')
    
    # Create ThreePointStats table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS ThreePointStats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER,
            season TEXT,
            three_pt_percentage REAL,
            three_pt_made INTEGER,
            three_pt_attempts INTEGER,
            FOREIGN KEY (team_id) REFERENCES Teams(team_id),
            UNIQUE(team_id, season)
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

def three_point_data(cur, conn, season, max_teams=25):
    """Get 3-point shooting percentage data for a specific season"""
    # Season code format example: "2019-20" for 2019-2020 season
    season_code = f"{season.split('-')[0]}-{season.split('-')[1][-2:]}"
    url = f"https://www.basketball-reference.com/leagues/NBA_{season.split('-')[0]}.html"
    
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the team stats table
    team_stats_table = soup.find('table', id='team-stats-per_game')
    
    if not team_stats_table:
        print(f"Could not find team stats for season {season}")
        return 0
    
    # Get all rows from the table
    rows = team_stats_table.find_all('tr')[1:]  # Skip header row
    
    counter = 0
    for row in rows:
        if counter >= max_teams:
            break
            
        cols = row.find_all('td')
        if not cols:
            continue
            
        team_name = cols[0].text.strip()
        
        # Find 3PT data (may need adjustment based on actual table structure)
        three_pt_made = float(cols[7].text) if cols[7].text else 0
        three_pt_attempts = float(cols[8].text) if cols[8].text else 0
        three_pt_percentage = float(cols[9].text) if cols[9].text else 0
        
        # Get team_id from the database
        cur.execute("SELECT team_id FROM Teams WHERE team_name = ?", (team_name,))
        result = cur.fetchone()
        
        if result:
            team_id = result[0]
            # Insert 3-point data
            cur.execute('''
                INSERT OR IGNORE INTO ThreePointStats 
                (team_id, season, three_pt_percentage, three_pt_made, three_pt_attempts)
                VALUES (?, ?, ?, ?, ?)
            ''', (team_id, season, three_pt_percentage, three_pt_made, three_pt_attempts))
            
            if cur.rowcount > 0:
                counter += 1
    
    conn.commit()
    return counter



def main():
    cur, conn = set_up_database("bball-stats.db")
    teams_added = get_teams_data(cur, conn)  
    
    seasons = [
        "2019-2020", 
        "2020-2021", 
        "2021-2022", 
        "2022-2023", 
        "2023-2024"
    ]
    
    for season in seasons:
        rows_added = three_point_data(cur, conn, season, max_teams=25)
    
    conn.close()


if __name__ == "__main__":
    main()
