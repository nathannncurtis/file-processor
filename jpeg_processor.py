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

            os.remove(destination_file)
            print(f"Removed original PDF: {destination_file}")

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
