import sqlite3
import os

def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()

    # Create Teams table if it doesn't exist (don't drop it)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Teams (
            team_id INTEGER PRIMARY KEY,
            team_name TEXT UNIQUE,
            team_abbreviation TEXT
        )
    ''')
    
    # Create season-specific ThreePointStats tables if they don't exist
    for season in ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS ThreePointStats_{season} (
                team_id INTEGER PRIMARY KEY,
                three_pt_percentage REAL,
                three_pt_made INTEGER,
                three_pt_attempts INTEGER,
                games_played INTEGER,
                FOREIGN KEY (team_id) REFERENCES Teams(team_id),
                UNIQUE(team_id)
            )
        ''')

    # Create season-specific WinStats tables if they don't exist
    for season in ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS WinStats_{season} (
                team_id INTEGER PRIMARY KEY,
                win_percentage REAL,
                wins INTEGER,
                losses INTEGER,
                FOREIGN KEY (team_id) REFERENCES Teams(team_id),
                UNIQUE(team_id)
            )
        ''')
    
    conn.commit()


    for season in ["2019_2020", "2020_2021", "2021_2022", "2022_2023", "2023_2024"]:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS DefensiveStats_{season} (
                team_id INTEGER PRIMARY KEY,
                defensive_rebounds REAL,
                steals REAL,
                blocks REAL,
                personal_fouls REAL,
                games_played INTEGER,
                FOREIGN KEY (team_id) REFERENCES Teams(team_id)
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

    for counter, team in enumerate(teams, 1):
        cur.execute('''
            INSERT OR IGNORE INTO Teams (team_id, team_name, team_abbreviation)
            VALUES (?, ?, ?)
        ''', (counter, team["name"], team["abbreviation"]))
    conn.commit()

if __name__ == "__main__":
    cur, conn = set_up_database("nba-stats.db")
    get_teams_data(cur, conn)
    print("Database setup complete. Teams table populated.")
    conn.close()