import requests
from datetime import datetime, timedelta
import os
from pathlib import Path
import concurrent.futures
from threading import Semaphore
from tqdm import tqdm
import logging
from ratelimit import limits, sleep_and_retry
import sys
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.prompt import Prompt, IntPrompt
from rich import print as rprint

console = Console()

# Configure logging with rich
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='download_log.txt'
)

def validate_channel(channel):
    """Check if the channel exists on logs.ivr.fi"""
    with console.status(f"[bold blue]Checking if {channel} logs are available..."):
        url = f"https://logs.ivr.fi/channel/{channel}"
        try:
            response = requests.get(url, allow_redirects=True)
            # Check if we got actual content rather than just status code
            return "No logs found" not in response.text and response.status_code == 200
        except requests.exceptions.RequestException:
            return False

# Rate limit: 5 calls per second maximum
@sleep_and_retry
@limits(calls=5, period=1)
def download_single_day(channel, date_str, semaphore):
    """Download logs for a single day with rate limiting"""
    current_date = datetime.strptime(date_str, "%Y-%m-%d")
    next_date = current_date + timedelta(days=1)

    # Format dates in RFC 3339 format
    from_date = current_date.strftime("%Y-%m-%dT00:00:00Z")
    to_date = next_date.strftime("%Y-%m-%dT00:00:00Z")

    filename = f"logs/{channel}_logs_{date_str}.txt"

    # Skip if file already exists
    if os.path.exists(filename):
        return f"Skipped {date_str} - file exists"

    with semaphore:
        try:
            url = f"https://logs.ivr.fi/channel/{channel}"
            params = {
                "from": from_date,
                "to": to_date
            }

            response = requests.get(url, params=params)
            response.raise_for_status()

            # Save to file
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(response.text)

            return f"Successfully downloaded {date_str}"

        except requests.exceptions.RequestException as e:
            error_msg = f"Error downloading {date_str}: {str(e)}"
            logging.error(error_msg)
            return error_msg
        except Exception as e:
            error_msg = f"Unexpected error for {date_str}: {str(e)}"
            logging.error(error_msg)
            return error_msg

def generate_date_list(start_date_str, end_date_str):
    """Generate a list of dates between start and end dates"""
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")

    date_list = []
    current_date = start_date

    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return date_list

def download_logs_parallel(channel, start_date_str, end_date_str, max_workers=10, max_concurrent_requests=5):
    """Download logs in parallel with rich progress display"""
    Path("logs").mkdir(exist_ok=True)
    dates = generate_date_list(start_date_str, end_date_str)
    semaphore = Semaphore(max_concurrent_requests)
    failed_downloads = {}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        task = progress.add_task(f"[cyan]Downloading {channel} logs", total=len(dates))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {
                executor.submit(download_single_day, channel, date, semaphore): date
                for date in dates
            }

            for future in concurrent.futures.as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    result = future.result()
                    if "Error" in result:
                        failed_downloads[date] = result
                except Exception as e:
                    failed_downloads[date] = str(e)

                progress.update(task, advance=1)

    if failed_downloads:
        console.print("\n[red]Failed downloads:[/red]")
        for date, error in failed_downloads.items():
            console.print(f"  • {date}: {error}")

        with open(f"failed_downloads_{channel}.txt", "w") as f:
            for date, error in failed_downloads.items():
                f.write(f"{date}: {error}\n")

        console.print(f"\n[yellow]Failed downloads have been saved to[/yellow] failed_downloads_{channel}.txt")

def get_valid_date(prompt):
    """Get a valid date input from the user"""
    while True:
        date_str = input(prompt)
        try:
            datetime.strptime(date_str, "%d/%m/%Y")
            return date_str
        except ValueError:
            print("Invalid date format. Please use DD/MM/YYYY")

if __name__ == "__main__":
    console.print(Panel.fit(
        "[bold blue]Twitch Chat Log Downloader[/bold blue]\n"
        "[dim]Downloads chat logs from logs.ivr.fi[/dim]"
    ))

    while True:
        channel = Prompt.ask("\n[bold cyan]Enter Twitch channel name").lower()
        
        if validate_channel(channel):
            console.print(f"\n[green]✓[/green] Channel [bold]{channel}[/bold] found!")
            break
        else:
            console.print(f"\n[red]✗[/red] Channel [bold]{channel}[/bold] not found on logs.ivr.fi")
            continue

    console.print("\n[bold cyan]Enter date range[/bold cyan] [dim](format: DD/MM/YYYY)[/dim]")
    START_DATE = get_valid_date("Start date")
    END_DATE = get_valid_date("End date")

    console.print(f"\n[bold green]Starting download for {channel}...[/bold green]")
    download_logs_parallel(
        channel,
        START_DATE,
        END_DATE,
        max_workers=10,
        max_concurrent_requests=5
    )
    console.print("\n[bold green]✓ Download process completed![/bold green]")