import sqlite3
import os

def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()

    # Create Seasons table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Seasons (
            season_id INTEGER PRIMARY KEY,
            season_name TEXT UNIQUE
        )
    ''')
    
    # Add seasons data
    seasons = [
        (1, "2019-2020"),
        (2, "2020-2021"),
        (3, "2021-2022"),
        (4, "2022-2023"),
        (5, "2023-2024")
    ]
    
    for season in seasons:
        cur.execute('''
            INSERT OR IGNORE INTO Seasons (season_id, season_name)
            VALUES (?, ?)
        ''', season)

    # Create Teams table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Teams (
            team_id INTEGER PRIMARY KEY,
            team_name TEXT UNIQUE,
            team_abbreviation TEXT
        )
    ''')
    
    # Create consolidated ThreePointStats table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS ThreePointStats (
            team_id INTEGER,
            season_id INTEGER,
            three_pt_percentage REAL,
            three_pt_made INTEGER,
            three_pt_attempts INTEGER,
            games_played INTEGER,
            PRIMARY KEY (team_id, season_id),
            FOREIGN KEY (team_id) REFERENCES Teams(team_id),
            FOREIGN KEY (season_id) REFERENCES Seasons(season_id)
        )
    ''')

    # Create consolidated WinStats table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS WinStats (
            team_id INTEGER,
            season_id INTEGER,
            win_percentage REAL,
            wins INTEGER,
            losses INTEGER,
            PRIMARY KEY (team_id, season_id),
            FOREIGN KEY (team_id) REFERENCES Teams(team_id),
            FOREIGN KEY (season_id) REFERENCES Seasons(season_id)
        )
    ''')
    
    # Create consolidated DefensiveStats table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS DefensiveStats (
            team_id INTEGER,
            season_id INTEGER,
            defensive_rebounds REAL,
            steals REAL,
            blocks REAL,
            personal_fouls REAL,
            games_played INTEGER,
            PRIMARY KEY (team_id, season_id),
            FOREIGN KEY (team_id) REFERENCES Teams(team_id),
            FOREIGN KEY (season_id) REFERENCES Seasons(season_id)
        )
    ''')
    
    conn.commit()
    return cur, conn

def get_teams_data(cur, conn):
    teams = [
        {"name": "Atlanta Hawks", "abbreviation": "ATL"},
        {"name": "Boston Celtics", "abbreviation": "BOS"},
        {"name": "Brooklyn Nets", "abbreviation": "BKN"},
        {"name": "Charlotte Hornets", "abbreviation": "CHA"},
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
        {"name": "Phoenix Suns", "abbreviation": "PHX"},
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