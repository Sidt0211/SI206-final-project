# Database creation
import sqlite3
import os

def create_database():
    """
    Creates the SQLite database and tables for storing NBA statistics.
    """
    # Create database if it doesn't exist
    conn = sqlite3.connect('nba_stats.db')
    cursor = conn.cursor()
    
    # Create Teams table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Teams (
        team_id INTEGER PRIMARY KEY,
        team_name TEXT UNIQUE,
        team_abbr TEXT UNIQUE
    )
    ''')
    
    # Create ThreePointStats table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ThreePointStats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER,
        season TEXT,
        three_pt_percentage REAL,
        win_percentage REAL,
        FOREIGN KEY (team_id) REFERENCES Teams(team_id),
        UNIQUE(team_id, season)
    )
    ''')
    
    # Create DefensiveStats table
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
    
    # Create PlayerEfficiency table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS PlayerEfficiency (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER,
        season TEXT,
        top_player_name TEXT,
        top_player_per REAL,
        win_percentage REAL,
        FOREIGN KEY (team_id) REFERENCES Teams(team_id),
        UNIQUE(team_id, season)
    )
    ''')
    
    # Insert NBA teams data
    nba_teams = [
        (1, "Atlanta Hawks", "ATL"),
        (2, "Boston Celtics", "BOS"),
        (3, "Brooklyn Nets", "BKN"),
        (4, "Charlotte Hornets", "CHA"),
        (5, "Chicago Bulls", "CHI"),
        (6, "Cleveland Cavaliers", "CLE"),
        (7, "Dallas Mavericks", "DAL"),
        (8, "Denver Nuggets", "DEN"),
        (9, "Detroit Pistons", "DET"),
        (10, "Golden State Warriors", "GSW"),
        (11, "Houston Rockets", "HOU"),
        (12, "Indiana Pacers", "IND"),
        (13, "Los Angeles Clippers", "LAC"),
        (14, "Los Angeles Lakers", "LAL"),
        (15, "Memphis Grizzlies", "MEM"),
        (16, "Miami Heat", "MIA"),
        (17, "Milwaukee Bucks", "MIL"),
        (18, "Minnesota Timberwolves", "MIN"),
        (19, "New Orleans Pelicans", "NOP"),
        (20, "New York Knicks", "NYK"),
        (21, "Oklahoma City Thunder", "OKC"),
        (22, "Orlando Magic", "ORL"),
        (23, "Philadelphia 76ers", "PHI"),
        (24, "Phoenix Suns", "PHX"),
        (25, "Portland Trail Blazers", "POR"),
        (26, "Sacramento Kings", "SAC"),
        (27, "San Antonio Spurs", "SAS"),
        (28, "Toronto Raptors", "TOR"),
        (29, "Utah Jazz", "UTA"),
        (30, "Washington Wizards", "WAS")
    ]
    
    # Insert teams data, ignoring if already exists
    for team in nba_teams:
        try:
            cursor.execute("INSERT INTO Teams (team_id, team_name, team_abbr) VALUES (?, ?, ?)", team)
        except sqlite3.IntegrityError:
            pass  # Skip if team already exists
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    print("Database and tables created successfully!")

if __name__ == "__main__":
    create_database()