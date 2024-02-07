from rich.progress import track, Progress
import time

iterator = ["a"] * 100

with Progress() as progress:
    # task = progress.add_task("[cyan]Processing...", total=100)
    for elem in progress.track(iterator, total=100):
        progress.update(progress.task_ids[0], description=f"fetching... {elem}")
        time.sleep(0.03)