import time
import os
import psutil
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import json
import re
from tabulate import tabulate
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PerformanceAnalyzer:
    def __init__(self, streaming_scripts, batch_scripts, output_dir="performance_results"):
        """
        Initialize the performance analyzer
        
        Args:
            streaming_scripts: List of paths to streaming processing scripts
            batch_scripts: List of paths to batch processing scripts
            output_dir: Directory to save results
        """
        self.streaming_scripts = streaming_scripts
        self.batch_scripts = batch_scripts
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Metrics storage
        self.streaming_metrics = []
        self.batch_metrics = []
        self.streaming_results = {}
        self.batch_results = {}
        
    def run_script(self, script_path, processing_type, timeout=120):
        """
        Run a script and monitor its performance
        
        Args:
            script_path: Path to the script
            processing_type: 'streaming' or 'batch'
            timeout: Maximum execution time in seconds
        
        Returns:
            Dict containing performance metrics
        """
        logger.info(f"Running {processing_type} script: {script_path}")
        
        # Start monitoring resources
        start_time = time.time()
        
        # Determine which command to use based on processing type
        if processing_type == 'streaming':
            cmd = ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", script_path]
        else:  # batch
            cmd = ["spark-submit", "--driver-class-path", 
                  "/home/hadoop/Downloads/mysql-connector-j_9.3.0-1ubuntu25.04_all/usr/share/java/mysql-connector-j-9.3.0.jar", 
                  "--jars", "/home/hadoop/Downloads/mysql-connector-j_9.3.0-1ubuntu25.04_all/usr/share/java/mysql-connector-j-9.3.0.jar", 
                  script_path]
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Initialize metrics
        metrics = {
            'script': os.path.basename(script_path),
            'type': processing_type,
            'start_time': start_time,
            'cpu_usage': [],
            'memory_usage': [],
            'execution_time': 0,
            'output': "",
            'error': "",
            'status': 'success'
        }
        
        # Monitor resource usage
        try:
            pid = process.pid
            p = psutil.Process(pid)
            
            # Monitor for timeout seconds or until process completes
            elapsed = 0
            while process.poll() is None and elapsed < timeout:
                try:
                    cpu_percent = p.cpu_percent(interval=1)
                    memory_info = p.memory_info()
                    
                    metrics['cpu_usage'].append(cpu_percent)
                    metrics['memory_usage'].append(memory_info.rss / (1024 * 1024))  # MB
                    
                    elapsed = time.time() - start_time
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    break
            
            # If process is still running after timeout, kill it
            if process.poll() is None:
                process.terminate()
                metrics['status'] = 'timeout'
                logger.warning(f"Script {script_path} timed out after {timeout} seconds")
            
            # Collect output
            stdout, stderr = process.communicate()
            metrics['output'] = stdout
            metrics['error'] = stderr
            
        except Exception as e:
            logger.error(f"Error monitoring script {script_path}: {str(e)}")
            metrics['status'] = 'error'
            metrics['error'] = str(e)
        
        metrics['execution_time'] = time.time() - start_time
        
        # Extract results from output
        if processing_type == 'streaming':
            self.streaming_results[os.path.basename(script_path)] = self.extract_results(metrics['output'])
        else:
            self.batch_results[os.path.basename(script_path)] = self.extract_results(metrics['output'])
        
        return metrics
    
    def extract_results(self, output):
        """
        Extract table results from Spark output
        
        Args:
            output: Script output as string
        
        Returns:
            Dict with table names and their data
        """
        results = {}
        
        # Find table sections in the output
        table_pattern = r"===+\s+(.*?)\s+===+\n(.*?)(?=\n===|\Z)"
        matches = re.finditer(table_pattern, output, re.DOTALL)
        
        for match in matches:
            table_name = match.group(1).strip()
            table_content = match.group(2).strip()
            
            # Parse table content into a structured format
            # This is a simplified approach - you may need to adjust based on your actual output format
            try:
                lines = table_content.strip().split('\n')
                if len(lines) < 2:  # Need at least headers and one row
                    continue
                
                # Simple parsing of table-like output
                headers = re.split(r'\s{2,}', lines[0].strip())
                data = []
                
                for line in lines[1:]:
                    if line.strip() and not line.startswith('+--'):
                        values = re.split(r'\s{2,}', line.strip())
                        if len(values) == len(headers):
                            data.append(values)
                
                results[table_name] = {
                    'headers': headers,
                    'data': data
                }
            except Exception as e:
                logger.warning(f"Error parsing table {table_name}: {str(e)}")
        
        return results
    
    def run_all_scripts(self):
        """Run all scripts and collect performance metrics"""
        logger.info("Starting performance analysis...")
        
        # Run streaming scripts
        for script in self.streaming_scripts:
            metrics = self.run_script(script, 'streaming')
            self.streaming_metrics.append(metrics)
        
        # Run batch scripts
        for script in self.batch_scripts:
            metrics = self.run_script(script, 'batch')
            self.batch_metrics.append(metrics)
        
        logger.info("Completed running all scripts")
    
    def calculate_metrics(self):
        """Calculate summary metrics for all runs"""
        streaming_summary = {
            'avg_execution_time': np.mean([m['execution_time'] for m in self.streaming_metrics]),
            'max_execution_time': np.max([m['execution_time'] for m in self.streaming_metrics]),
            'avg_cpu_usage': np.mean([np.mean(m['cpu_usage']) for m in self.streaming_metrics]),
            'avg_memory_usage': np.mean([np.mean(m['memory_usage']) for m in self.streaming_metrics]),
            'max_memory_usage': np.max([np.max(m['memory_usage']) for m in self.streaming_metrics]),
        }
        
        batch_summary = {
            'avg_execution_time': np.mean([m['execution_time'] for m in self.batch_metrics]),
            'max_execution_time': np.max([m['execution_time'] for m in self.batch_metrics]),
            'avg_cpu_usage': np.mean([np.mean(m['cpu_usage']) for m in self.batch_metrics]),
            'avg_memory_usage': np.mean([np.mean(m['memory_usage']) for m in self.batch_metrics]),
            'max_memory_usage': np.max([np.max(m['memory_usage']) for m in self.batch_metrics]),
        }
        
        return {
            'streaming': streaming_summary,
            'batch': batch_summary
        }
    
    def compare_results(self):
        """Compare results between streaming and batch processing"""
        comparison = {}
        
        # Map similar scripts between batch and streaming
        script_mapping = {
            'runrate.py': 'runrate_batch.py',
            'highest_score_team_match.py': 'highest_score_team_match_batch.py',
            'highest_score_overall.py': 'highest_score_overall_batch.py',
            'bowl.py': 'bowl_batch.py'
        }
        
        # Compare outputs
        for streaming_script, batch_script in script_mapping.items():
            if streaming_script in self.streaming_results and batch_script in self.batch_results:
                streaming_tables = self.streaming_results[streaming_script]
                batch_tables = self.batch_results[batch_script]
                
                # Find common table names
                common_tables = set(streaming_tables.keys()).intersection(set(batch_tables.keys()))
                
                script_comparison = {}
                for table in common_tables:
                    # Compare table structures and data
                    streaming_data = streaming_tables[table]
                    batch_data = batch_tables[table]
                    
                    # Check if headers match
                    headers_match = streaming_data['headers'] == batch_data['headers']
                    
                    # Compare data (simplified - just count rows)
                    streaming_rows = len(streaming_data['data'])
                    batch_rows = len(batch_data['data'])
                    
                    # For this analysis, we'll just check if top results match
                    # In a more sophisticated analysis, you might compare actual values
                    matching_rows = 0
                    if headers_match and streaming_rows > 0 and batch_rows > 0:
                        # Compare first few rows
                        rows_to_compare = min(streaming_rows, batch_rows, 5)
                        for i in range(rows_to_compare):
                            if i < streaming_rows and i < batch_rows:
                                if streaming_data['data'][i] == batch_data['data'][i]:
                                    matching_rows += 1
                    
                    script_comparison[table] = {
                        'headers_match': headers_match,
                        'streaming_rows': streaming_rows,
                        'batch_rows': batch_rows,
                        'matching_top_rows': matching_rows,
                        'match_percentage': matching_rows / min(streaming_rows, batch_rows) * 100 if min(streaming_rows, batch_rows) > 0 else 0
                    }
                
                comparison[f"{streaming_script} vs {batch_script}"] = script_comparison
        
        return comparison
    
    def generate_report(self):
        """Generate a comprehensive performance report"""
        # Calculate metrics
        metrics_summary = self.calculate_metrics()
        result_comparison = self.compare_results()
        
        # Create report
        report = {
            'execution_time_comparison': {
                'streaming': {m['script']: m['execution_time'] for m in self.streaming_metrics},
                'batch': {m['script']: m['execution_time'] for m in self.batch_metrics}
            },
            'resource_usage': {
                'streaming': {
                    'avg_cpu': metrics_summary['streaming']['avg_cpu_usage'],
                    'avg_memory_mb': metrics_summary['streaming']['avg_memory_usage'],
                    'max_memory_mb': metrics_summary['streaming']['max_memory_usage']
                },
                'batch': {
                    'avg_cpu': metrics_summary['batch']['avg_cpu_usage'],
                    'avg_memory_mb': metrics_summary['batch']['avg_memory_usage'],
                    'max_memory_mb': metrics_summary['batch']['max_memory_usage']
                }
            },
            'result_comparison': result_comparison
        }
        
        # Save report
        with open(f"{self.output_dir}/performance_report.json", 'w') as f:
            json.dump(report, f, indent=4)
        
        # Return report for further processing
        return report
    
    def plot_performance_charts(self):
        """Generate performance visualization charts"""
        # Execution time comparison
        streaming_times = [m['execution_time'] for m in self.streaming_metrics]
        batch_times = [m['execution_time'] for m in self.batch_metrics]
        streaming_labels = [m['script'] for m in self.streaming_metrics]
        batch_labels = [m['script'] for m in self.batch_metrics]
        
        # Execution time chart
        plt.figure(figsize=(12, 6))
        x = np.arange(len(streaming_labels))
        width = 0.35
        
        plt.bar(x - width/2, streaming_times, width, label='Streaming')
        plt.bar(x + width/2, [batch_times[i] if i < len(batch_times) else 0 for i in range(len(x))], width, label='Batch')
        
        plt.xlabel('Scripts')
        plt.ylabel('Execution Time (seconds)')
        plt.title('Execution Time Comparison: Streaming vs Batch')
        plt.xticks(x, streaming_labels, rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"{self.output_dir}/execution_time_comparison.png")
        
        # CPU usage over time (for the first script of each type)
        if self.streaming_metrics and self.batch_metrics:
            plt.figure(figsize=(12, 6))
            plt.plot(self.streaming_metrics[0]['cpu_usage'], label=f"Streaming: {self.streaming_metrics[0]['script']}")
            plt.plot(self.batch_metrics[0]['cpu_usage'], label=f"Batch: {self.batch_metrics[0]['script']}")
            plt.xlabel('Time (seconds)')
            plt.ylabel('CPU Usage (%)')
            plt.title('CPU Usage Over Time')
            plt.legend()
            plt.tight_layout()
            plt.savefig(f"{self.output_dir}/cpu_usage_comparison.png")
            
            # Memory usage over time
            plt.figure(figsize=(12, 6))
            plt.plot(self.streaming_metrics[0]['memory_usage'], label=f"Streaming: {self.streaming_metrics[0]['script']}")
            plt.plot(self.batch_metrics[0]['memory_usage'], label=f"Batch: {self.batch_metrics[0]['script']}")
            plt.xlabel('Time (seconds)')
            plt.ylabel('Memory Usage (MB)')
            plt.title('Memory Usage Over Time')
            plt.legend()
            plt.tight_layout()
            plt.savefig(f"{self.output_dir}/memory_usage_comparison.png")
        
        # Average resource usage comparison
        metrics_summary = self.calculate_metrics()
        
        # CPU and Memory bar chart
        plt.figure(figsize=(10, 6))
        metrics_labels = ['Avg CPU Usage (%)', 'Avg Memory (MB)', 'Max Memory (MB)']
        streaming_values = [
            metrics_summary['streaming']['avg_cpu_usage'],
            metrics_summary['streaming']['avg_memory_usage'],
            metrics_summary['streaming']['max_memory_usage']
        ]
        batch_values = [
            metrics_summary['batch']['avg_cpu_usage'],
            metrics_summary['batch']['avg_memory_usage'],
            metrics_summary['batch']['max_memory_usage']
        ]
        
        x = np.arange(len(metrics_labels))
        width = 0.35
        
        plt.bar(x - width/2, streaming_values, width, label='Streaming')
        plt.bar(x + width/2, batch_values, width, label='Batch')
        
        plt.xlabel('Metrics')
        plt.ylabel('Value')
        plt.title('Resource Usage Comparison')
        plt.xticks(x, metrics_labels)
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"{self.output_dir}/resource_usage_comparison.png")
    
    def create_html_report(self):
        """Create an HTML report with performance comparisons and visualizations"""
        metrics_summary = self.calculate_metrics()
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Spark Streaming vs Batch Performance Analysis</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1, h2, h3 {{ color: #00458f; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .chart-container {{ margin: 20px 0; }}
                .metric-box {{ 
                    border: 1px solid #ddd; 
                    border-radius: 5px; 
                    padding: 15px; 
                    margin-bottom: 20px;
                    background-color: #f9f9f9;
                }}
                .metric-value {{ 
                    font-size: 24px; 
                    font-weight: bold; 
                    color: #00458f;
                    margin: 10px 0;
                }}
                .metric-label {{ font-size: 14px; color: #666; }}
                .metric-grid {{ 
                    display: grid; 
                    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
                    gap: 20px;
                }}
            </style>
        </head>
        <body>
            <h1>Spark Streaming vs Batch Processing Performance Analysis</h1>
            
            <h2>Summary Metrics</h2>
            <div class="metric-grid">
                <div class="metric-box">
                    <div class="metric-label">Streaming Avg Execution Time</div>
                    <div class="metric-value">{metrics_summary['streaming']['avg_execution_time']:.2f}s</div>
                </div>
                <div class="metric-box">
                    <div class="metric-label">Batch Avg Execution Time</div>
                    <div class="metric-value">{metrics_summary['batch']['avg_execution_time']:.2f}s</div>
                </div>
                <div class="metric-box">
                    <div class="metric-label">Streaming Avg CPU Usage</div>
                    <div class="metric-value">{metrics_summary['streaming']['avg_cpu_usage']:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-label">Batch Avg CPU Usage</div>
                    <div class="metric-value">{metrics_summary['batch']['avg_cpu_usage']:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-label">Streaming Avg Memory Usage</div>
                    <div class="metric-value">{metrics_summary['streaming']['avg_memory_usage']:.2f} MB</div>
                </div>
                <div class="metric-box">
                    <div class="metric-label">Batch Avg Memory Usage</div>
                    <div class="metric-value">{metrics_summary['batch']['avg_memory_usage']:.2f} MB</div>
                </div>
            </div>
            
            <h2>Performance Charts</h2>
            <div class="chart-container">
                <h3>Execution Time Comparison</h3>
                <img src="execution_time_comparison.png" alt="Execution Time Comparison" style="width: 100%; max-width: 800px;">
            </div>
            
            <div class="chart-container">
                <h3>CPU Usage Over Time</h3>
                <img src="cpu_usage_comparison.png" alt="CPU Usage Comparison" style="width: 100%; max-width: 800px;">
            </div>
            
            <div class="chart-container">
                <h3>Memory Usage Over Time</h3>
                <img src="memory_usage_comparison.png" alt="Memory Usage Comparison" style="width: 100%; max-width: 800px;">
            </div>
            
            <div class="chart-container">
                <h3>Resource Usage Comparison</h3>
                <img src="resource_usage_comparison.png" alt="Resource Usage Comparison" style="width: 100%; max-width: 800px;">
            </div>
            
            <h2>Execution Time Details</h2>
            <table>
                <tr>
                    <th>Script Type</th>
                    <th>Script Name</th>
                    <th>Execution Time (s)</th>
                </tr>
        """
        
        # Add streaming script rows
        for metric in self.streaming_metrics:
            html_content += f"""
                <tr>
                    <td>Streaming</td>
                    <td>{metric['script']}</td>
                    <td>{metric['execution_time']:.2f}</td>
                </tr>
            """
        
        # Add batch script rows
        for metric in self.batch_metrics:
            html_content += f"""
                <tr>
                    <td>Batch</td>
                    <td>{metric['script']}</td>
                    <td>{metric['execution_time']:.2f}</td>
                </tr>
            """
        
        html_content += """
            </table>
            
            <h2>Result Comparison</h2>
            <p>Comparing streaming and batch processing results to validate consistency:</p>
        """
        
        # Add result comparison details
        result_comparison = self.compare_results()
        for script_pair, tables in result_comparison.items():
            html_content += f"""
            <h3>{script_pair}</h3>
            <table>
                <tr>
                    <th>Table Name</th>
                    <th>Headers Match</th>
                    <th>Streaming Rows</th>
                    <th>Batch Rows</th>
                    <th>Matching Top Rows</th>
                    <th>Match Percentage</th>
                </tr>
            """
            
            for table_name, comparison in tables.items():
                html_content += f"""
                <tr>
                    <td>{table_name}</td>
                    <td>{"Yes" if comparison['headers_match'] else "No"}</td>
                    <td>{comparison['streaming_rows']}</td>
                    <td>{comparison['batch_rows']}</td>
                    <td>{comparison['matching_top_rows']}</td>
                    <td>{comparison['match_percentage']:.2f}%</td>
                </tr>
                """
            
            html_content += "</table>"
        
        html_content += """
        </body>
        </html>
        """
        
        # Write HTML file
        with open(f"{self.output_dir}/performance_report.html", 'w') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated at {self.output_dir}/performance_report.html")

def main():
    """Main function to run the performance analyzer"""
    # Define paths to scripts
    streaming_dir = "./spark"
    batch_dir = "./batch"
    
    streaming_scripts = [
        f"{streaming_dir}/runrate.py",
        f"{streaming_dir}/highest_score_team_match.py",
        f"{streaming_dir}/highest_score_overall.py",
        f"{streaming_dir}/bowl.py"
    ]
    
    batch_scripts = [
        f"{batch_dir}/runrate_batch.py",
        f"{batch_dir}/highest_score_team_match_batch.py",
        f"{batch_dir}/highest_score_overall_batch.py",
        f"{batch_dir}/bowl_batch.py"
    ]
    
    # Create and run performance analyzer
    analyzer = PerformanceAnalyzer(streaming_scripts, batch_scripts)
    
    # Run all scripts and collect metrics
    analyzer.run_all_scripts()
    
    # Generate report and visualizations
    analyzer.generate_report()
    analyzer.plot_performance_charts()
    analyzer.create_html_report()
    
    logger.info("Performance analysis completed successfully")
    
    # Print summary statistics
    metrics = analyzer.calculate_metrics()
    print("\n===== PERFORMANCE SUMMARY =====")
    print(f"Streaming Avg Execution Time: {metrics['streaming']['avg_execution_time']:.2f}s")
    print(f"Batch Avg Execution Time: {metrics['batch']['avg_execution_time']:.2f}s")
    print(f"Streaming Avg CPU Usage: {metrics['streaming']['avg_cpu_usage']:.2f}%")
    print(f"Batch Avg CPU Usage: {metrics['batch']['avg_cpu_usage']:.2f}%")
    print(f"Streaming Avg Memory Usage: {metrics['streaming']['avg_memory_usage']:.2f} MB")
    print(f"Batch Avg Memory Usage: {metrics['batch']['avg_memory_usage']:.2f} MB")
    
    # Print speed improvement
    if metrics['batch']['avg_execution_time'] > 0:
        improvement = (metrics['batch']['avg_execution_time'] - metrics['streaming']['avg_execution_time']) / metrics['batch']['avg_execution_time'] * 100
        if improvement > 0:
            print(f"\nStreaming is {improvement:.2f}% faster than batch processing")
        else:
            print(f"\nBatch is {-improvement:.2f}% faster than streaming processing")

if __name__ == "__main__":
    main()
