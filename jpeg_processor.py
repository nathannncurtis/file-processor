import os
import time
import shutil
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import fitz
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

    def wait_for_folder_stability(self, folder_path, stability_duration=10):
        """Ensure the folder is stable (no changes) for a specified duration before processing."""
        stable_start_time = None

        while True:
            try:
                initial_snapshot = {f: os.path.getsize(os.path.join(folder_path, f)) for f in os.listdir(folder_path)}
            except FileNotFoundError:
                print(f"Folder not found: {folder_path}. Retrying...")
                return False

            time.sleep(5)
            try:
                current_snapshot = {f: os.path.getsize(os.path.join(folder_path, f)) for f in os.listdir(folder_path)}
            except FileNotFoundError:
                print(f"Folder not found: {folder_path}. Retrying...")
                return False

            if initial_snapshot == current_snapshot:
                if stable_start_time is None:
                    stable_start_time = time.time()
                    print("No changes detected in folder. Starting stability timer.")
                elif time.time() - stable_start_time >= stability_duration:
                    print("Folder stable for required duration.")
                    return True
            else:
                stable_start_time = None
                print("Changes detected in folder. Restarting stability check.")

    def merge_folders(self, src_folder, dest_folder):
        """Merge src_folder into dest_folder, handling potential filename conflicts and permissions."""
        if not os.path.exists(dest_folder):
            try:
                shutil.move(src_folder, dest_folder)
            except PermissionError:
                print(f"Permission denied when trying to move {src_folder} to {dest_folder}. Retrying...")
                time.sleep(5)
                shutil.move(src_folder, dest_folder)
        else:
            for root, _, files in os.walk(src_folder):
                relative_path = os.path.relpath(root, src_folder)
                target_folder = os.path.join(dest_folder, relative_path)
                os.makedirs(target_folder, exist_ok=True)

                for file in files:
                    src_file = os.path.join(root, file)
                    dest_file = os.path.join(target_folder, file)

                    # Check if file exists before moving
                    if not os.path.exists(src_file):
                        print(f"File not found: {src_file}. Skipping.")
                        continue

                    if os.path.exists(dest_file):
                        # Handle conflicts by renaming or using a subfolder
                        conflict_folder = os.path.join(target_folder, "conflicts")
                        os.makedirs(conflict_folder, exist_ok=True)
                        dest_file = os.path.join(conflict_folder, file)

                    try:
                        shutil.move(src_file, dest_file)
                    except PermissionError:
                        print(f"Permission denied when trying to move {src_file} to {dest_file}. Retrying...")
                        time.sleep(5)
                        shutil.move(src_file, dest_file)

            # Clean up the original source folder if empty
            shutil.rmtree(src_folder, ignore_errors=True)

    def process_directory(self, folder_path):
        """Process files in the folder and ensure they are handled safely."""
        if not self.wait_for_folder_stability(folder_path, stability_duration=30):
            print(f"Stability check failed for folder: {folder_path}")
            return

        # Verify folder exists before attempting to process
        if not os.path.exists(folder_path):
            print(f"Folder no longer exists: {folder_path}. Skipping processing.")
            return

        # Process each file in the folder
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            if file.lower().endswith((".jpeg", ".jpg")):
                self.process_jpeg(file_path)
            elif file.lower().endswith(".pdf"):
                self.process_pdf(file_path)

        # Merge or move the processed folder to the output directory
        destination_folder = os.path.join(self.output_directory, os.path.basename(folder_path))
        self.merge_folders(folder_path, destination_folder)
        print(f"Folder processed and merged to output: {destination_folder}")

    def process_pdf(self, pdf_file):
        """Converts each page of the PDF to a JPEG file and removes the original PDF after processing."""
        try:
            # Verify the file still exists before processing
            if not os.path.exists(pdf_file):
                print(f"File no longer exists: {pdf_file}. Skipping processing.")
                return

            doc = fitz.open(pdf_file)
            total_pages = len(doc)
            page_digits = len(str(total_pages))

            print(f"Processing {total_pages} pages in PDF: {pdf_file}")
            for page_num in range(total_pages):
                page = doc.load_page(page_num)
                pix = page.get_pixmap(dpi=200)

                img = Image.open(io.BytesIO(pix.tobytes("ppm")))
                if img.mode != "RGB":
                    img = img.convert("RGB")

                output_jpeg = os.path.join(
                    os.path.dirname(pdf_file),
                    f"{os.path.splitext(os.path.basename(pdf_file))[0]}_page_{str(page_num + 1).zfill(page_digits)}.jpg"
                )
                img.save(output_jpeg, "JPEG", quality=60, dpi=(200, 200))
                print(f"Saved JPEG: {output_jpeg}")

            doc.close()
            print(f"Finished processing PDF: {pdf_file}")

            # Remove the original PDF after processing, with a verification check
            if os.path.exists(pdf_file):
                os.remove(pdf_file)
                print(f"Removed original PDF: {pdf_file}")

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
