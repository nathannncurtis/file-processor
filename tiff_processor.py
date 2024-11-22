import sys
import os
import time
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import fitz  # PyMuPDF
from PIL import Image
import io
import argparse
import logging

# Configure logging
logging.basicConfig(
    filename="tiff_processor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def parse_args():
    parser = argparse.ArgumentParser(description="TIFF Processor for PDFs and JPEGs")
    parser.add_argument('--watch-dir', required=True, help='Directory to watch for new folders with PDFs and JPEGs')
    parser.add_argument('--output-dir', required=True, help='Directory to move completed folders')
    parser.add_argument('--max-retries', type=int, default=10, help='Maximum number of retries for processing a file')
    return parser.parse_args()

class PDFJPEGHandler(FileSystemEventHandler):
    def __init__(self, output_directory, watch_directory, max_retries=10):
        self.output_directory = output_directory
        self.watch_directory = watch_directory
        self.max_retries = max_retries

    def on_created(self, event):
        if event.is_directory:
            logging.info(f"New folder detected: {event.src_path}")
            self.process_directory(event.src_path)

    def wait_for_folder_stability(self, folder_path, stability_duration=10):
        stable_start_time = None
        while True:
            try:
                initial_files = {f: os.path.getsize(os.path.join(folder_path, f)) for f in os.listdir(folder_path)}
            except FileNotFoundError:
                logging.warning(f"Folder not found: {folder_path}. Retrying...")
                return False

            time.sleep(2)
            try:
                current_files = {f: os.path.getsize(os.path.join(folder_path, f)) for f in os.listdir(folder_path)}
            except FileNotFoundError:
                logging.warning(f"Folder not found: {folder_path}. Retrying...")
                return False

            if initial_files == current_files:
                if stable_start_time is None:
                    stable_start_time = time.time()
                elif time.time() - stable_start_time >= stability_duration:
                    return True
            else:
                stable_start_time = None

    def process_directory(self, folder_path):
        if not self.wait_for_folder_stability(folder_path):
            logging.warning(f"Stability check failed for folder: {folder_path}")
            return

        all_files = os.listdir(folder_path)
        jpeg_files = [os.path.join(folder_path, f) for f in all_files if f.lower().endswith((".jpeg", ".jpg"))]
        pdf_files = [os.path.join(folder_path, f) for f in all_files if f.lower().endswith(".pdf")]

        # Process JPEG files
        for jpeg_file in jpeg_files:
            if self.process_jpeg(jpeg_file):
                os.remove(jpeg_file)
                logging.info(f"Deleted processed JPEG: {jpeg_file}")

        # Process PDF files
        for pdf_file in pdf_files:
            pdf_result = self.process_pdf(pdf_file)
            if not pdf_result['failed_pages']:
                logging.info(f"Successfully processed PDF: {pdf_file}")
                try:
                    os.remove(pdf_file)
                    logging.info(f"Deleted processed PDF: {pdf_file}")
                except Exception as e:
                    logging.error(f"Failed to delete PDF {pdf_file}: {e}")

        # Move folder after processing
        destination_folder = os.path.join(self.output_directory, os.path.basename(folder_path))
        self.move_folder(folder_path, destination_folder)

    def move_folder(self, src_folder, dest_folder):
        if not os.path.exists(dest_folder):
            shutil.move(src_folder, dest_folder)
            logging.info(f"Moved folder: {src_folder} -> {dest_folder}")
        else:
            for root, _, files in os.walk(src_folder):
                target_folder = os.path.join(dest_folder, os.path.relpath(root, src_folder))
                os.makedirs(target_folder, exist_ok=True)
                for file in files:
                    shutil.move(os.path.join(root, file), os.path.join(target_folder, file))
            shutil.rmtree(src_folder)

    def process_pdf(self, pdf_file):
        """Converts each page of the PDF to a TIFF file and deletes the PDF after successful processing."""
        processed_pages = []
        failed_pages = []
        
        try:
            doc = fitz.open(pdf_file)
            total_pages = len(doc)
            logging.info(f"Processing {total_pages} pages in PDF: {pdf_file}")

            for page_num in range(total_pages):
                try:
                    # Load the page and create a Pixmap
                    page = doc[page_num]
                    pix = page.get_pixmap(dpi=200)

                    # Convert the Pixmap to a Pillow Image
                    img = Image.open(io.BytesIO(pix.tobytes("ppm"))).convert("L")
                    img = img.point(lambda x: 0 if x < 128 else 255, "1")  # Binarize (1-bit black & white)

                    # Save as TIFF with Group 4 compression
                    output_tiff = os.path.join(
                        os.path.dirname(pdf_file),
                        f"{os.path.splitext(os.path.basename(pdf_file))[0]}_page_{page_num + 1:04d}.tif"
                    )
                    img.save(output_tiff, "TIFF", compression="group4", dpi=(200, 200))
                    logging.info(f"Saved TIFF: {output_tiff}")
                    processed_pages.append(output_tiff)

                except Exception as e:
                    logging.error(f"Error processing page {page_num + 1} of {pdf_file}: {e}")
                    failed_pages.append(page_num + 1)

            doc.close()

            if not failed_pages:
                try:
                    os.remove(pdf_file)
                    logging.info(f"Deleted successfully processed PDF: {pdf_file}")
                except Exception as e:
                    logging.error(f"Failed to delete PDF {pdf_file}: {e}")

        except Exception as e:
            logging.error(f"Critical error processing PDF to TIFF {pdf_file}: {e}")
            failed_pages.append(f"Critical error: {e}")

        return {'processed_pages': processed_pages, 'failed_pages': failed_pages}

    def process_jpeg(self, jpeg_file):
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                img = Image.open(jpeg_file).convert("L")
                img = img.point(lambda x: 0 if x < 128 else 255, "1")
                output_tiff = os.path.join(
                    os.path.dirname(jpeg_file),
                    f"{os.path.splitext(os.path.basename(jpeg_file))[0]}.tif"
                )
                img.save(output_tiff, "TIFF", compression="group4", dpi=(200, 200))
                return True
            except Exception as e:
                retry_count += 1
                time.sleep(1)
        logging.error(f"Failed to process JPEG {jpeg_file} after {self.max_retries} retries.")
        return False

if __name__ == "__main__":
    args = parse_args()
    watch_directory = args.watch_dir
    output_directory = args.output_dir
    max_retries = args.max_retries

    if not os.path.exists(watch_directory):
        logging.error(f"Watch directory does not exist: {watch_directory}")
        sys.exit()

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    event_handler = PDFJPEGHandler(output_directory, watch_directory, max_retries)
    observer = Observer()
    observer.schedule(event_handler, watch_directory, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
