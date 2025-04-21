import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import sys
import os

# Add parent directory to path for importing BatchProcessor
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from batch.batch_processor import BatchProcessor

class IPLVisualizer:
    def __init__(self):
        self.processor = BatchProcessor()
        self.colors = sns.color_palette('husl', 8)
        plt.style.use('seaborn')

    def plot_top_batsmen(self, limit=5):
        """Plot top batsmen by runs scored"""
        data = self.processor.get_top_performers('batsmen', limit)
        names = [row[0] for row in data]
        runs = [row[1] for row in data]
        strike_rates = [row[2] for row in data]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Runs scored bar plot
        ax1.bar(names, runs, color=self.colors)
        ax1.set_title('Top Batsmen by Runs Scored')
        ax1.set_xlabel('Player Name')
        ax1.set_ylabel('Total Runs')
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # Strike rate line plot
        ax2.plot(names, strike_rates, marker='o', color=self.colors[1])
        ax2.set_title('Strike Rates of Top Batsmen')
        ax2.set_xlabel('Player Name')
        ax2.set_ylabel('Strike Rate')
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

        plt.tight_layout()
        plt.savefig('visualization/top_batsmen.svg')
        plt.close()

    def plot_top_bowlers(self, limit=5):
        """Plot top bowlers by wickets taken"""
        data = self.processor.get_top_performers('bowlers', limit)
        names = [row[0] for row in data]
        wickets = [row[1] for row in data]
        economy = [row[2] for row in data]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Wickets taken bar plot
        ax1.bar(names, wickets, color=self.colors)
        ax1.set_title('Top Bowlers by Wickets Taken')
        ax1.set_xlabel('Player Name')
        ax1.set_ylabel('Wickets')
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # Economy rate line plot
        ax2.plot(names, economy, marker='o', color=self.colors[2])
        ax2.set_title('Economy Rates of Top Bowlers')
        ax2.set_xlabel('Player Name')
        ax2.set_ylabel('Economy Rate')
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

        plt.tight_layout()
        plt.savefig('visualization/top_bowlers.svg')
        plt.close()

    def plot_team_performance(self, limit=8):
        """Plot team performance metrics"""
        data = self.processor.get_top_performers('teams', limit)
        teams = [row[0] for row in data]
        wins = [row[1] for row in data]
        win_percentages = [row[2] for row in data]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Matches won bar plot
        ax1.bar(teams, wins, color=self.colors)
        ax1.set_title('Team Performance by Matches Won')
        ax1.set_xlabel('Team')
        ax1.set_ylabel('Matches Won')
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # Win percentage line plot
        ax2.plot(teams, win_percentages, marker='o', color=self.colors[3])
        ax2.set_title('Team Win Percentages')
        ax2.set_xlabel('Team')
        ax2.set_ylabel('Win Percentage')
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

        plt.tight_layout()
        plt.savefig('visualization/team_performance.svg')
        plt.close()

    def generate_all_visualizations(self):
        """Generate all visualization plots"""
        os.makedirs('visualization', exist_ok=True)
        self.plot_top_batsmen()
        self.plot_top_bowlers()
        self.plot_team_performance()

if __name__ == '__main__':
    visualizer = IPLVisualizer()
    visualizer.generate_all_visualizations()