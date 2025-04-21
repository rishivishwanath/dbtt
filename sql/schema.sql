-- Create database for IPL analytics
CREATE DATABASE IF NOT EXISTS ipl_analytics;
USE ipl_analytics;

-- Matches table to store match-level information
CREATE TABLE IF NOT EXISTS matches (
    match_id INT PRIMARY KEY,
    season INT,
    match_no VARCHAR(10),
    date DATE,
    venue VARCHAR(100)
);

-- Deliveries table to store ball-by-ball data
CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id INT AUTO_INCREMENT PRIMARY KEY,
    match_id INT,
    innings INT,
    over_num INT,
    ball_num INT,
    striker VARCHAR(100),
    non_striker VARCHAR(100),
    bowler VARCHAR(100),
    runs_off_bat INT,
    extras INT,
    wide_runs INT,
    noball_runs INT,
    penalty_runs INT,
    wicket_type VARCHAR(50),
    player_dismissed VARCHAR(100),
    fielder VARCHAR(100),
    batting_team VARCHAR(100),
    bowling_team VARCHAR(100),
    FOREIGN KEY (match_id) REFERENCES matches(match_id)
);

-- Player statistics table for aggregated player performance
CREATE TABLE IF NOT EXISTS player_stats (
    player_id INT AUTO_INCREMENT PRIMARY KEY,
    player_name VARCHAR(100),
    matches_played INT DEFAULT 0,
    runs_scored INT DEFAULT 0,
    wickets_taken INT DEFAULT 0,
    batting_average DECIMAL(10,2),
    bowling_average DECIMAL(10,2),
    strike_rate DECIMAL(10,2),
    economy_rate DECIMAL(10,2)
);

-- Team statistics table for team-level analytics
CREATE TABLE IF NOT EXISTS team_stats (
    team_id INT AUTO_INCREMENT PRIMARY KEY,
    team_name VARCHAR(100),
    matches_played INT DEFAULT 0,
    matches_won INT DEFAULT 0,
    total_runs_scored INT DEFAULT 0,
    total_runs_conceded INT DEFAULT 0,
    net_run_rate DECIMAL(10,3)
);