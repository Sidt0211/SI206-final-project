import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os

def calculate_and_visualize_3pt_win_correlation(db_name="nba-stats.db"):
    conn = sqlite3.connect(db_name)

    query = """
    SELECT 
        t.team_id,
        t.team_name,
        t.team_abbreviation,
        AVG(tp.three_pt_percentage) as avg_3pt_pct,
        AVG(ws.win_percentage) as avg_win_pct
    FROM 
        Teams t
    JOIN 
        ThreePointStats tp ON t.team_id = tp.team_id
    JOIN 
        WinStats ws ON t.team_id = ws.team_id AND tp.season_id = ws.season_id
    GROUP BY
        t.team_id, t.team_name, t.team_abbreviation
    ORDER BY
        avg_win_pct DESC
    """
    
    df = pd.read_sql_query(query, conn)
    correlation = np.corrcoef(df['avg_3pt_pct'], df['avg_win_pct'])[0, 1]
    
    # Write out the calculated data to a file as text
    with open('out-3pt.txt', 'w') as f:
        f.write("NBA 3-Point Shooting and Win Percentage Analysis\n")
        f.write(f"Correlation between 3PT% and Win%: {correlation:.4f}\n\n")
        f.write("Team Statistics (Ranked by Win Percentage):\n")
        f.write(f"{'Team':<25} {'Abbr':<5} {'Avg 3PT%':<10} {'Avg Win%':<10}\n")
        f.write("-" * 55 + "\n")
        
        for _, row in df.iterrows():
            f.write(f"{row['team_name']:<25} {row['team_abbreviation']:<5} ")
            f.write(f"{row['avg_3pt_pct']:.4f}    {row['avg_win_pct']:.4f}\n")
        
        # Add interpretation
        if correlation > 0.7:
            f.write("\nInterpretation: There is a strong positive correlation between ")
            f.write("3-point shooting percentage and win percentage.\n")
        elif correlation > 0.4:
            f.write("\nInterpretation: There is a moderate positive correlation between ")
            f.write("3-point shooting percentage and win percentage.\n")
        else:
            f.write("\nInterpretation: There is a weak correlation between ")
            f.write("3-point shooting percentage and win percentage.\n")
    
    plt.figure(figsize=(12, 8))
    
    scatter = plt.scatter(
        df['avg_3pt_pct'], 
        df['avg_win_pct'],
        s=100, 
        c=df['avg_3pt_pct'],
        cmap='viridis', 
        alpha=0.8,
        edgecolors='black'
    )
    
    # Add regression line
    x = df['avg_3pt_pct']
    y = df['avg_win_pct']
    m, b = np.polyfit(x, y, 1)
    plt.plot(x, m*x + b, 'r-', linewidth=2, label=f'y = {m:.4f}x + {b:.4f}')
    
    # Add team labels
    for i, row in df.iterrows():
        plt.annotate(
            row['team_abbreviation'],
            (row['avg_3pt_pct'], row['avg_win_pct']),
            fontsize=10,
            fontweight='bold',
            ha='center',
            va='center',
            color='white',
            bbox=dict(boxstyle="round,pad=0.3", fc='navy', alpha=0.7)
        )
    
    # Add correlation coefficient text
    plt.text(
        0.05, 0.95, 
        f"Correlation: {correlation:.4f}", 
        transform=plt.gca().transAxes,
        fontsize=12, 
        bbox=dict(facecolor='white', alpha=0.7)
    )
    
    
    # Add details and styling
    plt.title('NBA 3-Point Shooting vs. Win Percentage (2019-2024)', fontsize=16)
    plt.xlabel('Average 3-Point Percentage', fontsize=14)
    plt.ylabel('Average Win Percentage', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Add quadrant lines at median values
    median_3pt = df['avg_3pt_pct'].median()
    median_win = df['avg_win_pct'].median()
    
    plt.axvline(x=median_3pt, color='gray', linestyle='--', alpha=0.5)
    plt.axhline(y=median_win, color='gray', linestyle='--', alpha=0.5)
    
    # Used AI to help me divide the graph into quadrants and annotate them
    plt.text(df['avg_3pt_pct'].min() + 0.005, median_win + (df['avg_win_pct'].max() - median_win)/2, 
             "Low 3PT%\nHigh Win%", ha='left', va='center', fontsize=10,
             bbox=dict(facecolor='white', alpha=0.5))
             
    plt.text(median_3pt + (df['avg_3pt_pct'].max() - median_3pt)/2, median_win + (df['avg_win_pct'].max() - median_win)/2, 
             "High 3PT%\nHigh Win%", ha='center', va='center', fontsize=10,
             bbox=dict(facecolor='white', alpha=0.5))
             
    plt.text(df['avg_3pt_pct'].min() + 0.005, median_win - (median_win - df['avg_win_pct'].min())/2, 
             "Low 3PT%\nLow Win%", ha='left', va='center', fontsize=10,
             bbox=dict(facecolor='white', alpha=0.5))
             
    plt.text(median_3pt + (df['avg_3pt_pct'].max() - median_3pt)/2, median_win - (median_win - df['avg_win_pct'].min())/2, 
             "High 3PT%\nLow Win%", ha='center', va='center', fontsize=10,
             bbox=dict(facecolor='white', alpha=0.5))
    
    # Save and display the figure
    plt.tight_layout()
    plt.savefig('3pt_win_correlation.png', dpi=300, bbox_inches='tight')
    
    conn.close()
    return plt


def calculate_and_visualize_defense_win_correlation(db_name="nba-stats.db"):

    conn = sqlite3.connect(db_name)
    query = """
    SELECT 
        t.team_id,
        t.team_name,
        t.team_abbreviation,
        AVG(ds.defensive_rebounds / ds.games_played) as avg_def_reb_per_game,
        AVG(ds.steals / ds.games_played) as avg_steals_per_game,
        AVG(ds.blocks / ds.games_played) as avg_blocks_per_game,
        AVG(ds.personal_fouls / ds.games_played) as avg_fouls_per_game,
        AVG(ws.win_percentage) as avg_win_pct
    FROM 
        Teams t
    JOIN 
        DefensiveStats ds ON t.team_id = ds.team_id
    JOIN 
        WinStats ws ON t.team_id = ws.team_id AND ds.season_id = ws.season_id
    GROUP BY
        t.team_id, t.team_name, t.team_abbreviation
    ORDER BY
        avg_win_pct DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    # Calculate a composite defensive rating
    # Weighted formula: (2 × blocks + 1.5 × steals + 0.5 × defensive rebounds - 0.25 × fouls)
    df['defensive_rating'] = (
        2.0 * df['avg_blocks_per_game'] + 
        1.5 * df['avg_steals_per_game'] + 
        0.5 * df['avg_def_reb_per_game'] - 
        0.25 * df['avg_fouls_per_game']
    )
    
    correlation = np.corrcoef(df['defensive_rating'], df['avg_win_pct'])[0, 1]
    correlations = {
        'Defensive Rebounds': np.corrcoef(df['avg_def_reb_per_game'], df['avg_win_pct'])[0, 1],
        'Steals': np.corrcoef(df['avg_steals_per_game'], df['avg_win_pct'])[0, 1],
        'Blocks': np.corrcoef(df['avg_blocks_per_game'], df['avg_win_pct'])[0, 1],
        'Personal Fouls': np.corrcoef(df['avg_fouls_per_game'], df['avg_win_pct'])[0, 1],
        'Composite Rating': correlation
    }
    
    # Write out the calculated data to a file as text
    with open('out-defense.txt', 'w') as f:
        f.write("NBA Defensive Statistics and Win Percentage Analysis\n")
        
        # Write correlation statistics
        f.write("Correlation with Win Percentage:\n")
        for stat, corr in correlations.items():
            f.write(f"- {stat}: {corr:.4f}\n")
        f.write("\n")
        
        # Write team-by-team statistics
        f.write("Team Statistics (Ranked by Win Percentage):\n")
        f.write(f"{'Team':<25} {'Abbr':<5} {'Def Rating':<10} {'Reb/G':<8} {'Stl/G':<8} {'Blk/G':<8} {'Win%':<8}\n")
        f.write("-" * 75 + "\n")
        
        for _, row in df.iterrows():
            f.write(f"{row['team_name']:<25} {row['team_abbreviation']:<5} ")
            f.write(f"{row['defensive_rating']:.4f}    ")
            f.write(f"{row['avg_def_reb_per_game']:.2f}    ")
            f.write(f"{row['avg_steals_per_game']:.2f}    ")
            f.write(f"{row['avg_blocks_per_game']:.2f}    ")
            f.write(f"{row['avg_win_pct']:.4f}\n")
        
        # Add interpretation
        if correlation > 0.7:
            f.write("\nInterpretation: There is a strong positive correlation between ")
            f.write("defensive rating and win percentage.\n")
        elif correlation > 0.4:
            f.write("\nInterpretation: There is a moderate positive correlation between ")
            f.write("defensive rating and win percentage.\n")
        else:
            f.write("\nInterpretation: There is a weak correlation between ")
            f.write("defensive rating and win percentage.\n")
    
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(
        df['defensive_rating'], 
        df['avg_win_pct'],
        s=100, 
        c=df['defensive_rating'], 
        cmap='coolwarm',  
        alpha=0.8, 
        edgecolors='black' 
    )
    
    x = df['defensive_rating']
    y = df['avg_win_pct']
    m, b = np.polyfit(x, y, 1)
    plt.plot(x, m*x + b, 'g-', linewidth=2, label=f'y = {m:.4f}x + {b:.4f}')
    
    for i, row in df.iterrows():
        plt.annotate(
            row['team_abbreviation'],
            (row['defensive_rating'], row['avg_win_pct']),
            fontsize=10,
            fontweight='bold',
            ha='center',
            va='center',
            color='white',
            bbox=dict(boxstyle="round,pad=0.3", fc='darkgreen', alpha=0.7)
        )
    
    plt.text(
        0.05, 0.95, 
        f"Correlation: {correlation:.4f}", 
        transform=plt.gca().transAxes,
        fontsize=12, 
        bbox=dict(facecolor='white', alpha=0.7)
    )
    
    plt.title('NBA Defensive Rating vs. Win Percentage (2019-2024)', fontsize=16)
    plt.xlabel('Defensive Rating', fontsize=14)
    plt.ylabel('Average Win Percentage', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    median_def = df['defensive_rating'].median()
    median_win = df['avg_win_pct'].median()
    
    plt.axvline(x=median_def, color='gray', linestyle='--', alpha=0.5)
    plt.axhline(y=median_win, color='gray', linestyle='--', alpha=0.5)
    plt.text(df['defensive_rating'].min() + 0.1, median_win + (df['avg_win_pct'].max() - median_win)/2, 
             "Low Defense\nHigh Win%", ha='left', va='center', fontsize=10,
             bbox=dict(facecolor='white', alpha=0.5))
             
    plt.text(median_def + (df['defensive_rating'].max() - median_def)/2, median_win + (df['avg_win_pct'].max() - median_win)/2, 
             "High Defense\nHigh Win%", ha='center', va='center', fontsize=10,
             bbox=dict(facecolor='white', alpha=0.5))
             
    plt.text(df['defensive_rating'].min() + 0.1, median_win - (median_win - df['avg_win_pct'].min())/2, 
             "Low Defense\nLow Win%", ha='left', va='center', fontsize=10,
             bbox=dict(facecolor='white', alpha=0.5))
             
    plt.text(median_def + (df['defensive_rating'].max() - median_def)/2, median_win - (median_win - df['avg_win_pct'].min())/2, 
             "High Defense\nLow Win%", ha='center', va='center', fontsize=10,
             bbox=dict(facecolor='white', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig('defense_win_correlation.png', dpi=300, bbox_inches='tight')
    
    conn.close()
    return plt

def calculate_and_visualize_team_efficiency(db_name="nba-stats.db"):
    """
    Calculate average player efficiency for each team and create a visualization
    comparing teams against each other.
    """
    conn = sqlite3.connect(db_name)
    player_query = """
    SELECT 
        t.team_id,
        t.team_name,
        t.team_abbreviation,
        pe.player_name,
        pe.efficiency
    FROM 
        Teams t
    JOIN 
        PlayerEfficiency pe ON t.team_id = pe.team_id
    ORDER BY
        t.team_id, pe.efficiency DESC
    """
    
    # Execute query and load results into DataFrame
    player_df = pd.read_sql_query(player_query, conn)
    
    team_efficiencies = []
    for team_id in player_df['team_id'].unique():
        team_players = player_df[player_df['team_id'] == team_id].sort_values('efficiency', ascending=False)
        team_name = team_players.iloc[0]['team_name']
        team_abbr = team_players.iloc[0]['team_abbreviation']
        
        top_players = team_players.head(5)
        avg_efficiency = top_players['efficiency'].mean()
        
        team_efficiencies.append({
            'team_id': team_id,
            'team_name': team_name,
            'team_abbreviation': team_abbr,
            'avg_efficiency': avg_efficiency,
            'player_count': len(top_players),
            'players': top_players[['player_name', 'efficiency']].to_dict('records')
        })
    
    efficiency_df = pd.DataFrame(team_efficiencies)
    
    # Write out the calculated data to a file as text
    with open('out-eff.txt', 'w') as f:
        f.write("NBA Team Player Efficiency Statst\n")
        
        # Sort by efficiency
        sorted_teams = efficiency_df.sort_values('avg_efficiency', ascending=False)
        
        for i, (idx, row) in enumerate(sorted_teams.iterrows(), 1):
            f.write(f"\n{i}. {row['team_name']} ({row['team_abbreviation']}):\n")
            f.write(f"  Average Efficiency: {row['avg_efficiency']:.2f}\n")
            f.write(f"  Top Players:\n")
            
            for j, player in enumerate(row['players'], 1):
                f.write(f"    {j}. {player['player_name']}: {player['efficiency']:.2f}\n")
        
        # Add some statistics
        f.write(f"\nLeague Average Efficiency: {sorted_teams['avg_efficiency'].mean():.2f}\n")
        f.write(f"Highest Team Efficiency: {sorted_teams['avg_efficiency'].max():.2f} ({sorted_teams.iloc[0]['team_name']})\n")
        f.write(f"Lowest Team Efficiency: {sorted_teams['avg_efficiency'].min():.2f} ({sorted_teams.iloc[-1]['team_name']})\n")
    
    # Create the visualization - horizontal bar chart
    plt.figure(figsize=(12, 10))
    
    # Sort by efficiency for the bar chart
    sorted_df = efficiency_df.sort_values('avg_efficiency', ascending=False)
    
    # Create horizontal bar chart of team efficiencies
    bars = plt.barh(
        sorted_df['team_abbreviation'], 
        sorted_df['avg_efficiency'],
        color=plt.cm.viridis(np.linspace(0, 1, len(sorted_df))),
        height=0.7,
        edgecolor='black',
        alpha=0.8
    )
    
    # Add value labels at end of bars
    for bar in bars:
        width = bar.get_width()
        plt.text(
            width + 0.1,
            bar.get_y() + bar.get_height()/2.,
            f'{width:.2f}',
            ha='left', 
            va='center',
            fontweight='bold'
        )
    
    # Add vertical line at average efficiency
    avg_eff = sorted_df['avg_efficiency'].mean()
    plt.axvline(x=avg_eff, color='red', linestyle='--', alpha=0.7, 
                label=f'League Average: {avg_eff:.2f}')
    
    # Add details and styling
    plt.title('NBA Team Player Efficiency Comparison (Top 5 Players)', fontsize=16)
    plt.xlabel('Average Player Efficiency Rating', fontsize=14)
    plt.ylabel('Team', fontsize=14)
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.legend()
    
    # Add background color for groups of teams (top third, middle third, bottom third)
    n_teams = len(sorted_df)
    tier_size = n_teams // 3
    
    # Get y-coordinates
    y_coords = range(len(sorted_df))
    
    # Add tier labels and backgrounds
    plt.axhspan(y_coords[0] - 0.4, y_coords[tier_size-1] + 0.4, alpha=0.1, color='green', label='Top Tier')
    plt.axhspan(y_coords[tier_size] - 0.4, y_coords[2*tier_size-1] + 0.4, alpha=0.1, color='yellow', label='Middle Tier')
    plt.axhspan(y_coords[2*tier_size] - 0.4, y_coords[-1] + 0.4, alpha=0.1, color='red', label='Bottom Tier')
    
    plt.tight_layout()
    plt.savefig('team_efficiency_comparison.png', dpi=300, bbox_inches='tight')
    
    conn.close()
    return plt


# Execute the function if this script is run directly
if __name__ == "__main__":
    plt = calculate_and_visualize_3pt_win_correlation()
    plt.show()
    plt = calculate_and_visualize_defense_win_correlation()
    plt.show()
    plt = calculate_and_visualize_team_efficiency()
    plt.show()
