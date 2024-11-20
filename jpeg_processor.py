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
import logging

# Configure logging
logging.basicConfig(
    filename="jpeg_processor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def parse_args():
    parser = argparse.ArgumentParser(description="JPEG Processor")
    parser.add_argument('--watch-dir', required=True, help='Directory to watch for new PDFs')
    parser.add_argument('--output-dir', required=True, help='Directory to move completed files')
    return parser.parse_args()

class PDFHandler(FileSystemEventHandler):
    def __init__(self, output_directory):
        self.output_directory = output_directory

    def on_created(self, event):
        """Triggered when a new file or folder is created."""
        if event.is_directory:
            logging.info(f"New folder detected: {event.src_path}")
            self.process_directory(event.src_path)
        else:
            logging.info(f"New file detected: {event.src_path}")
            if event.src_path.lower().endswith(".pdf"):
                logging.info(f"PDF detected, starting processing: {event.src_path}")
                self.process_pdf(event.src_path)

    def wait_for_file_stability(self, file_path, stability_duration=10):
        """Ensure the file is stable before processing."""
        stable_start_time = None
        while True:
            try:
                initial_size = os.path.getsize(file_path)
            except FileNotFoundError:
                logging.warning(f"File not found: {file_path}. Retrying...")
                return False

            time.sleep(2)  # Check every 2 seconds

            try:
                current_size = os.path.getsize(file_path)
            except FileNotFoundError:
                logging.warning(f"File not found: {file_path}. Retrying...")
                return False

            if initial_size == current_size:
                if stable_start_time is None:
                    stable_start_time = time.time()
                    logging.info(f"No changes detected for {file_path}. Stability timer started.")
                elif time.time() - stable_start_time >= stability_duration:
                    logging.info(f"File stable for {stability_duration} seconds: {file_path}")
                    return True
            else:
                stable_start_time = None
                logging.info(f"Changes detected in {file_path}. Restarting stability timer.")

    def process_directory(self, folder_path):
        """Process all PDFs in a stable folder."""
        if not self.wait_for_folder_stability(folder_path):
            logging.warning(f"Stability check failed for folder: {folder_path}")
            return

        destination_folder = os.path.join(self.output_directory, os.path.basename(folder_path))
        self.move_folder(folder_path, destination_folder)

        logging.info(f"Moved folder to output directory: {destination_folder}")

        for file in os.listdir(destination_folder):
            file_path = os.path.join(destination_folder, file)
            if file.lower().endswith(".pdf"):
                self.process_pdf(file_path)
            else:
                logging.info(f"Skipping unsupported file: {file_path}")

    def move_folder(self, src_folder, dest_folder):
        """Move folder and merge if destination exists."""
        if not os.path.exists(dest_folder):
            shutil.move(src_folder, dest_folder)
            logging.info(f"Folder moved: {src_folder} -> {dest_folder}")
        else:
            for root, _, files in os.walk(src_folder):
                relative_path = os.path.relpath(root, src_folder)
                target_folder = os.path.join(dest_folder, relative_path)
                os.makedirs(target_folder, exist_ok=True)

                for file in files:
                    src_file = os.path.join(root, file)
                    dest_file = os.path.join(target_folder, file)
                    if os.path.exists(dest_file):
                        dest_file = os.path.join(target_folder, f"conflict_{file}")
                    shutil.move(src_file, dest_file)
                    logging.info(f"File moved: {src_file} -> {dest_file}")
            shutil.rmtree(src_folder)
            logging.info(f"Source folder cleaned: {src_folder}")

    def process_pdf(self, pdf_file):
        """Convert each page of a PDF to JPEG."""
        try:
            if not self.wait_for_file_stability(pdf_file, 10):
                logging.warning(f"File not stable: {pdf_file}")
                return

            doc = fitz.open(pdf_file)
            total_pages = len(doc)
            logging.info(f"Processing {total_pages} pages in PDF: {pdf_file}")

            for page_num in range(total_pages):
                try:
                    page = doc[page_num]
                    pix = page.get_pixmap(dpi=200)

                    output_jpeg = os.path.join(
                        os.path.dirname(pdf_file),
                        f"{os.path.splitext(os.path.basename(pdf_file))[0]}_page_{page_num + 1}.jpg"
                    )

                    img = Image.open(io.BytesIO(pix.tobytes("ppm"))).convert("RGB")
                    img.save(output_jpeg, "JPEG", quality=60, dpi=(200, 200))
                    logging.info(f"Saved JPEG: {output_jpeg}")
                except Exception as e:
                    logging.error(f"Error processing page {page_num + 1} of {pdf_file}: {e}")
            doc.close()
            os.remove(pdf_file)
            logging.info(f"Removed original PDF: {pdf_file}")
        except Exception as e:
            logging.error(f"Error processing PDF {pdf_file}: {e}")

if __name__ == "__main__":
    args = parse_args()
    watch_directory = args.watch_dir
    output_directory = args.output_dir

    if not os.path.exists(watch_directory):
        logging.error(f"Watch directory does not exist: {watch_directory}")
        sys.exit()

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        logging.info(f"Created output directory: {output_directory}")

    event_handler = PDFHandler(output_directory)
    observer = Observer()
    observer.schedule(event_handler, watch_directory, recursive=True)
    observer.start()
    logging.info("Observer started...")

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
        logging.info("Observer stopped.")
    observer.join()
    logging.info("Observer joined and exiting.")
