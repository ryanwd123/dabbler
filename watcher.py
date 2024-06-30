import sys
import time
import subprocess
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path




script = sys.argv[1]


print('count of args:', len(sys.argv))

for arg in sys.argv:
    print(arg)


if len(sys.argv) == 3:
    path = Path(sys.argv[2])
else:
    path = Path(".")
    


class ChangeHandler(FileSystemEventHandler):
    def __init__(self):
        self.last_modified = {}
        self.process = None
        # self.last_modified = time.time()


        self.run_script()

    def run_script(self):
        if self.process:
            self.process.terminate()
            print("Killed process:", self.process.pid)

        self.process = subprocess.Popen(['py', script], env=os.environ, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        print("Started process:", self.process.pid)

    def on_modified(self, event):
        if event.is_directory:
            return

        current_time = time.time()
        file_path = event.src_path
        if '__pycache__' in file_path or '.py' not in file_path:
            return

        # Check if the file was modified recently
        if file_path in self.last_modified:
            if current_time - self.last_modified[file_path] < 1:
                return

        self.last_modified[file_path] = current_time
        print(f"File modified: {file_path}")
        
        self.run_script()

if __name__ == "__main__":
    process = None

    # path = "."  # The directory to monitor
    event_handler = ChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    
    observer.start()
    print(f"Monitoring changes in directory: {path}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        if event_handler.process:
            event_handler.process.terminate()
            print("Killed process:", event_handler.process.pid)
    
    observer.join()
