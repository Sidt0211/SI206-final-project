import requests
import sqlite3
import time
from datetime import datetime

# RapidAPI configuration
api_key = "6ae1fbcc24mshe9cca7641e6cf24p1ddf00jsn503fd8228252"
host = "free-nba.p.rapidapi.com"

headers = {
    "X-RapidAPI-Key": api_key,
    "X-RapidAPI-Host": host
}

# Set up database
def setup_database():
    conn = sqlite3.connect('nba_stats.db')
    cursor = conn.cursor()
    
    # Create teams table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS teams (
        id INTEGER PRIMARY KEY,
        name TEXT,
        abbreviation TEXT,
        city TEXT
    )
    ''')
    
    # Create stats table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS team_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER,
        team_name TEXT,
        season TEXT,
        win_percentage REAL,
        defensive_rating REAL,
        games_played INTEGER,
        FOREIGN KEY (team_id) REFERENCES teams (id)
    )
    ''')
    
    conn.commit()
    return conn, cursor

# Function to get all NBA teams
def get_teams():
    url = "https://free-nba.p.rapidapi.com/teams"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()['data']
    else:
        print(f"Error fetching teams: {response.status_code}")
        return None

# Function to get games for a specific season and team
def get_team_games(team_id, season, per_page=100):
    all_games = []
    page = 1
    
    while True:
        url = "https://free-nba.p.rapidapi.com/games"
        querystring = {
            "page": str(page),
            "per_page": str(per_page),
            "team_ids[]": str(team_id),
            "seasons[]": str(season)
        }
        
        response = requests.get(url, headers=headers, params=querystring)
        
        if response.status_code == 200:
            data = response.json()
            games = data['data']
            all_games.extend(games)
            
            # Check if we've fetched all pages
            if len(games) < per_page:
                break
            
            page += 1
            time.sleep(1)  # Avoid rate limiting
        else:
            print(f"Error fetching games: {response.status_code}")
            break
    
    return all_games

# Function to calculate defensive rating and win percentage
def calculate_team_stats(team_id, games):
    wins = 0
    total_games = 0
    points_allowed = 0
    
    for game in games:
        home_team = game['home_team']
        visitor_team = game['visitor_team']
        
        # Determine if our team is home or away
        is_home = home_team['id'] == team_id
        
        our_score = game['home_team_score'] if is_home else game['visitor_team_score']
        other_score = game['visitor_team_score'] if is_home else game['home_team_score']
        
        # Count wins
        if our_score > other_score:
            wins += 1
        
        # Track points allowed
        points_allowed += other_score
        total_games += 1
    
    # Calculate stats
    win_percentage = (wins / total_games) * 100 if total_games > 0 else 0
    defensive_rating = points_allowed / total_games if total_games > 0 else 0
    
    return {
        'win_percentage': win_percentage,
        'defensive_rating': defensive_rating,
        'games_played': total_games
    }

# Function to store teams in the database
def store_teams(cursor, teams):
    for team in teams:
        cursor.execute(
            "INSERT OR REPLACE INTO teams (id, name, abbreviation, city) VALUES (?, ?, ?, ?)",
            (team['id'], team['full_name'], team['abbreviation'], team['city'])
        )

# Function to store team stats in the database
def store_team_stats(cursor, team_id, team_name, season, stats):
    cursor.execute(
        """
        INSERT INTO team_stats 
        (team_id, team_name, season, win_percentage, defensive_rating, games_played) 
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            team_id, 
            team_name, 
            season, 
            stats['win_percentage'], 
            stats['defensive_rating'], 
            stats['games_played']
        )
    )

# Function to get summary statistics from the database
def get_summary_statistics(cursor):
    print("\n--- Summary Statistics ---")
    
    # Average defensive rating and win percentage by season
    print("\nSeason Averages:")
    cursor.execute("""
    SELECT season, 
           AVG(defensive_rating) as avg_defensive_rating, 
           AVG(win_percentage) as avg_win_percentage
    FROM team_stats
    GROUP BY season
    ORDER BY season
    """)
    
    seasons = cursor.fetchall()
    for season, def_rating, win_pct in seasons:
        print(f"Season {season}: Avg Defensive Rating: {def_rating:.2f}, Avg Win Percentage: {win_pct:.2f}%")
    
    # Best defensive teams by season
    print("\nBest Defensive Teams by Season:")
    cursor.execute("""
    SELECT t1.season, t1.team_name, t1.defensive_rating, t1.win_percentage
    FROM team_stats t1
    JOIN (
        SELECT season, MIN(defensive_rating) as min_rating
        FROM team_stats
        GROUP BY season
    ) t2
    ON t1.season = t2.season AND t1.defensive_rating = t2.min_rating
    ORDER BY t1.season
    """)
    
    best_defense = cursor.fetchall()
    for season, team, def_rating, win_pct in best_defense:
        print(f"Season {season}: {team} - Defensive Rating: {def_rating:.2f}, Win Percentage: {win_pct:.2f}%")
    
    # Teams with highest win percentage by season
    print("\nTeams with Highest Win Percentage by Season:")
    cursor.execute("""
    SELECT t1.season, t1.team_name, t1.win_percentage, t1.defensive_rating
    FROM team_stats t1
    JOIN (
        SELECT season, MAX(win_percentage) as max_win_pct
        FROM team_stats
        GROUP BY season
    ) t2
    ON t1.season = t2.season AND t1.win_percentage = t2.max_win_pct
    ORDER BY t1.season
    """)
    
    best_record = cursor.fetchall()
    for season, team, win_pct, def_rating in best_record:
        print(f"Season {season}: {team} - Win Percentage: {win_pct:.2f}%, Defensive Rating: {def_rating:.2f}")
    
    # Correlation between defensive rating and win percentage
    # Note: SQLite doesn't have a built-in correlation function,
    # so we'll use a simple approximation based on aggregates
    cursor.execute("""
    SELECT 
        COUNT(*) as n,
        SUM(defensive_rating) as sum_x,
        SUM(win_percentage) as sum_y,
        SUM(defensive_rating*defensive_rating) as sum_xx,
        SUM(win_percentage*win_percentage) as sum_yy,
        SUM(defensive_rating*win_percentage) as sum_xy
    FROM team_stats
    """)
    
    n, sum_x, sum_y, sum_xx, sum_yy, sum_xy = cursor.fetchone()
    
    # Calculate correlation coefficient
    numerator = n * sum_xy - sum_x * sum_y
    denominator = ((n * sum_xx - sum_x * sum_x) * (n * sum_yy - sum_y * sum_y)) ** 0.5
    
    if denominator != 0:
        correlation = numerator / denominator
        print(f"\nCorrelation between defensive rating and win percentage: {correlation:.4f}")
        if correlation < 0:
            print("This negative correlation suggests that teams with lower defensive ratings (fewer points allowed per game) tend to have higher win percentages.")
    else:
        print("\nCould not calculate correlation.")

# Main function to collect stats for all teams across seasons
def collect_nba_stats():
    # Set up database connection
    conn, cursor = setup_database()
    
    # Get all teams
    teams = get_teams()
    if not teams:
        conn.close()
        return
    
    # Store teams in database
    store_teams(cursor, teams)
    conn.commit()
    
    # Define the seasons we want to analyze
    seasons = ["2019", "2020", "2021", "2022", "2023", "2024"]
    
    # Process each team and season
    for team in teams:
        team_id = team['id']
        team_name = team['full_name']
        print(f"Processing {team_name}...")
        
        for season in seasons:
            print(f"  Season {season}...")
            
            # Get all games for this team and season
            games = get_team_games(team_id, season)
            
            # Skip if no games found
            if not games:
                print(f"  No games found for {team_name} in {season}")
                continue
            
            # Calculate stats
            stats = calculate_team_stats(team_id, games)
            
            # Store stats in database
            store_team_stats(cursor, team_id, team_name, season, stats)
            conn.commit()
            
            # Avoid hitting rate limits
            time.sleep(1)
    
    # Generate summary statistics
    get_summary_statistics(cursor)
    
    # Close database connection
    conn.close()
    print(f"\nData collection complete. Results stored in nba_stats.db")

# Run the collection
if __name__ == "__main__":
    print("Starting NBA stats collection...")
    collect_nba_stats()
