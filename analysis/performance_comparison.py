import mysql.connector
import pandas as pd
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PerformanceComparator:
    def __init__(self, host='localhost', user='root', password='', database='ipl_analytics'):
        self.db_config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }

    def run_query(self, query, mode='batch'):
        start_time = time.time()
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            execution_time = time.time() - start_time
            logger.info(f"{mode.capitalize()} mode query executed in {execution_time:.2f} seconds")
            return results, execution_time
        except Exception as e:
            logger.error(f"Error executing {mode} query: {str(e)}")
            return None, None
        finally:
            cursor.close()
            conn.close()

    def compare_queries(self):
        comparisons = [
            {
                'name': 'Highest Run Scorers',
                'query': """
                    SELECT player_name, runs_scored
                    FROM player_stats
                    WHERE runs_scored > 0
                    ORDER BY runs_scored DESC
                    LIMIT 10
                """
            },
            {
                'name': 'Top Wicket Takers',
                'query': """
                    SELECT player_name, wickets_taken
                    FROM player_stats
                    WHERE wickets_taken > 0
                    ORDER BY wickets_taken DESC
                    LIMIT 10
                """
            },
            {
                'name': 'Team Performance',
                'query': """
                    SELECT team_name, matches_played, total_runs_scored
                    FROM team_stats
                    ORDER BY total_runs_scored DESC
                """
            }
        ]

        results = []
        for comparison in comparisons:
            logger.info(f"\nExecuting {comparison['name']} comparison...")
            
            # Run in batch mode
            batch_results, batch_time = self.run_query(comparison['query'], 'batch')
            
            # Run in streaming mode (same query but measured separately)
            stream_results, stream_time = self.run_query(comparison['query'], 'streaming')
            
            if batch_results and stream_results:
                # Convert results to dataframes for comparison
                batch_df = pd.DataFrame(batch_results)
                stream_df = pd.DataFrame(stream_results)
                
                # Calculate accuracy (matching results percentage)
                accuracy = self.calculate_accuracy(batch_df, stream_df)
                
                results.append({
                    'analysis_type': comparison['name'],
                    'batch_time': batch_time,
                    'stream_time': stream_time,
                    'accuracy': accuracy,
                    'batch_records': len(batch_results),
                    'stream_records': len(stream_results)
                })

        return pd.DataFrame(results)

    def calculate_accuracy(self, batch_df, stream_df):
        """Calculate accuracy by comparing batch and streaming results"""
        if batch_df.empty or stream_df.empty:
            return 0.0
        
        try:
            # Reset index to ensure proper comparison
            batch_df = batch_df.reset_index(drop=True)
            stream_df = stream_df.reset_index(drop=True)
            
            # Calculate the percentage of matching records
            matching_records = (batch_df == stream_df).all(axis=1).sum()
            total_records = len(batch_df)
            
            return (matching_records / total_records) * 100 if total_records > 0 else 0.0
        except Exception as e:
            logger.error(f"Error calculating accuracy: {str(e)}")
            return 0.0

    def generate_report(self):
        """Generate a comprehensive comparison report"""
        results_df = self.compare_queries()
        
        # Calculate average metrics
        avg_metrics = {
            'avg_batch_time': results_df['batch_time'].mean(),
            'avg_stream_time': results_df['stream_time'].mean(),
            'avg_accuracy': results_df['accuracy'].mean()
        }
        
        # Print report
        print("\n=== Performance Comparison Report ===")
        print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        print("Detailed Analysis Results:")
        print(results_df.to_string(index=False))
        
        print("\nSummary Metrics:")
        print(f"Average Batch Processing Time: {avg_metrics['avg_batch_time']:.2f} seconds")
        print(f"Average Streaming Processing Time: {avg_metrics['avg_stream_time']:.2f} seconds")
        print(f"Average Accuracy: {avg_metrics['avg_accuracy']:.2f}%")
        
        return results_df, avg_metrics

if __name__ == "__main__":
    comparator = PerformanceComparator()
    comparator.generate_report()