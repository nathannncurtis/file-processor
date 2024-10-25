<<<<<<< HEAD
import os
import time
import shutil
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import fitz  # PyMuPDF
from PIL import Image
import io
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="JPEG Processor")
    parser.add_argument('--watch-dir', required=True, help='Directory to watch for new PDFs')
    parser.add_argument('--output-dir', required=True, help='Directory to move completed files')
    return parser.parse_args()

class PDFHandler(FileSystemEventHandler):
    def __init__(self, output_directory):
        self.output_directory = output_directory

    def on_created(self, event):
        print(f"Event detected: {event}")
        if event.is_directory:
            print(f"Folder created: {event.src_path}")
            self.process_directory(event.src_path)
        else:
            print(f"File created: {event.src_path}")
            if event.src_path.lower().endswith(".pdf"):
                print(f"PDF detected: {event.src_path}, starting processing.")
                self.process_pdf(event.src_path)

    def process_directory(self, folder_path):
        print(f"Processing directory: {folder_path}")
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith(".pdf"):
                    full_path = os.path.join(root, file)
                    print(f"PDF found in directory: {full_path}")
                    self.process_pdf(full_path)

    def process_pdf(self, pdf_file):
        print(f"Starting PDF processing: {pdf_file}")
        file_ready = False
        retry_count = 0
        max_retries = 5
        stable_duration = 5

        while not file_ready and retry_count < max_retries:
            try:
                initial_size = os.path.getsize(pdf_file)
                print(f"Initial size: {initial_size}")
                time.sleep(stable_duration)
                current_size = os.path.getsize(pdf_file)
                print(f"Current size after {stable_duration} seconds: {current_size}")
                if initial_size == current_size:
                    file_ready = True
                    print(f"File is ready for processing: {pdf_file}")
                else:
                    retry_count += 1
                    print(f"File still changing, retrying {retry_count}/{max_retries}")
            except FileNotFoundError:
                print(f"File not found, skipping: {pdf_file}")
                return

        if not file_ready:
            print(f"File not ready after retries, skipping: {pdf_file}")
            return

        relative_path = os.path.relpath(os.path.dirname(pdf_file), watch_directory)
        destination_folder = os.path.join(self.output_directory, relative_path)
        os.makedirs(destination_folder, exist_ok=True)
        print(f"Created destination folder: {destination_folder}")

        destination_file = os.path.join(destination_folder, os.path.basename(pdf_file))
        shutil.move(pdf_file, destination_file)
        print(f"Moved PDF to destination: {destination_file}")

        try:
            doc = fitz.open(destination_file)
            total_pages = len(doc)
            page_digits = len(str(total_pages))

            print(f"Processing {total_pages} pages in PDF: {destination_file}")
            for page_num in range(total_pages):
                page = doc.load_page(page_num)
                pix = page.get_pixmap(dpi=200)

                img = Image.open(io.BytesIO(pix.tobytes("ppm")))
                if img.mode != "RGB":
                    img = img.convert("RGB")

                output_jpeg = os.path.join(destination_folder, f"{os.path.splitext(os.path.basename(pdf_file))[0]}_page_{str(page_num + 1).zfill(page_digits)}.jpg")
                img.save(output_jpeg, "JPEG", quality=60, dpi=(200, 200))
                print(f"Saved JPEG: {output_jpeg}")

            doc.close()
            print(f"Finished processing PDF: {destination_file}")
        except Exception as e:
            print(f"Error processing PDF to JPEG: {e}")

if __name__ == "__main__":
    args = parse_args()
    watch_directory = args.watch_dir
    output_directory = args.output_dir

    print(f"Watching directory: {watch_directory}")
    print(f"Output directory: {output_directory}")

    if not os.path.exists(watch_directory):
        print(f"Watch directory does not exist: {watch_directory}")
        exit()

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print(f"Created output directory: {output_directory}")

    event_handler = PDFHandler(output_directory)
    observer = Observer()
    observer.schedule(event_handler, watch_directory, recursive=True)
    observer.start()
    print("Observer started...")

    try:
        while True:
            time.sleep(10)
            print("Observer is running...")
    except KeyboardInterrupt:
        observer.stop()
        print("Observer stopped by user.")
    observer.join()
    print("Observer joined and exiting.")
=======
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import json
import shutil
import subprocess

class JobManager:
    def __init__(self, config_file, max_workers=4):
        """Initialize the JobManager with a path to the config file and worker limit."""
        self.config_file = config_file
        self.processes = {}  # Keep track of running processes (one per job)
        self.load_config()
        self.executor = ProcessPoolExecutor(max_workers=max_workers)  # Limit the number of active workers
        self.active_jobs = {}  # Keep track of active futures

    def load_config(self):
        """Loads the configuration from the JSON file."""
        if not os.path.exists(self.config_file):
            self.config = {"network_folder": "", "profiles": {}}
            self.save_config()
        else:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)

    def save_config(self):
        """Saves the current configuration to the JSON file."""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=4)

    def add_profile(self, profile_name):
        """Adds a new job profile, creates necessary directories, and starts the processors."""
        base_folder = os.path.join(self.config['network_folder'], profile_name)
        jpeg_folder = os.path.join(base_folder, "JPEG")
        tiff_folder = os.path.join(base_folder, "TIFF")
        complete_folder = os.path.join(base_folder, "COMPLETE")

        os.makedirs(jpeg_folder, exist_ok=True)
        os.makedirs(tiff_folder, exist_ok=True)
        os.makedirs(complete_folder, exist_ok=True)

        self.config['profiles'][profile_name] = {
            "JPEG": jpeg_folder,
            "TIFF": tiff_folder,
            "COMPLETE": complete_folder,
            "status": "Active"
        }
        self.save_config()

        # Add job to the pool (executor) and store the future
        future_jpeg = self.executor.submit(self.start_processor, profile_name, "jpeg_processor.py", jpeg_folder, complete_folder)
        future_tiff = self.executor.submit(self.start_processor, profile_name, "tiff_processor.py", tiff_folder, complete_folder)

        self.active_jobs[profile_name] = (future_jpeg, future_tiff)

    def start_processor(self, profile_name, processor_name, watch_dir, output_dir):
        """Start a processor (JPEG/TIFF) using the .py file."""
        log_file = f"{profile_name}_{processor_name}.log"
        with open(log_file, "w") as log:
            try:
                process = subprocess.Popen(
                    ['python', processor_name, '--watch-dir', watch_dir, '--output-dir', output_dir],
                    stdout=log,
                    stderr=log
                )
                print(f"Started {processor_name} for profile {profile_name}, log output in {log_file}")
                process.wait()  # Wait for the process to complete before the future is marked as done
                print(f"Completed {processor_name} for profile {profile_name}")
            except Exception as e:
                print(f"Error starting {processor_name} for profile {profile_name}: {e}")

    def remove_profile(self, profile_name):
        """Removes a job profile and its corresponding directories."""
        base_folder = os.path.join(self.config['network_folder'], profile_name)
        if os.path.exists(base_folder):
            shutil.rmtree(base_folder)

        if profile_name in self.config['profiles']:
            del self.config['profiles'][profile_name]
            self.save_config()

        # Cancel any active jobs for this profile if they haven't finished yet
        if profile_name in self.active_jobs:
            future_jpeg, future_tiff = self.active_jobs.pop(profile_name, (None, None))
            if future_jpeg:
                future_jpeg.cancel()
            if future_tiff:
                future_tiff.cancel()

    def shutdown(self):
        """Shutdown the pool gracefully and wait for jobs to complete."""
        print("Shutting down job manager, waiting for running jobs to finish.")
        self.executor.shutdown(wait=True)
        print("All jobs have completed.")

if __name__ == "__main__":
    job_manager = JobManager("config.json", max_workers=4)
    job_manager.shutdown() 
>>>>>>> b61d9cc (final version)
