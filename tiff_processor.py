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

def parse_args():
    parser = argparse.ArgumentParser(description="TIFF Processor for PDFs and JPEGs")
    parser.add_argument('--watch-dir', required=True, help='Directory to watch for new folders with PDFs and JPEGs')
    parser.add_argument('--output-dir', required=True, help='Directory to move completed folders')
    return parser.parse_args()

class PDFJPEGHandler(FileSystemEventHandler):
    def __init__(self, output_directory, watch_directory):
        self.output_directory = output_directory
        self.watch_directory = watch_directory

    def on_created(self, event):
        """Triggered when a new folder is created."""
        if event.is_directory:
            print(f"New folder detected: {event.src_path}")
            self.process_directory(event.src_path)

    def wait_for_folder_stability(self, folder_path, stability_duration=5):
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

    def process_directory(self, folder_path):
        """Process each file in the folder after ensuring stability."""
        if not self.wait_for_folder_stability(folder_path, stability_duration=30):
            print(f"Stability check failed for folder: {folder_path}")
            return

        # Move the folder to the output directory
        destination_folder = os.path.join(self.output_directory, os.path.basename(folder_path))
        self.move_folder(folder_path, destination_folder)

        print(f"Moved folder to output directory: {destination_folder}")

        # Recheck stability after move
        if not self.wait_for_folder_stability(destination_folder, 10):
            print(f"Folder not stable after move: {destination_folder}")
            return

        # Process files in the destination folder
        for root, _, files in os.walk(destination_folder):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    if file.lower().endswith(".pdf"):
                        print(f"Processing PDF: {file_path}")
                        self.process_pdf(file_path)
                    elif file.lower().endswith((".jpeg", ".jpg")):
                        print(f"Processing JPEG: {file_path}")
                        self.process_jpeg(file_path)
                    else:
                        print(f"Skipping unsupported file: {file_path}")
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

        print(f"Completed processing for folder: {destination_folder}")

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

    def process_pdf(self, pdf_file):
        """Converts each page of a PDF to a TIFF file and removes the original PDF."""
        try:
            # Ensure file stability before processing
            if not self.wait_for_file_stability(pdf_file, 10):
                print(f"File not stable: {pdf_file}")
                return

            doc = fitz.open(pdf_file)
            total_pages = len(doc)
            print(f"Processing {total_pages} pages in PDF: {pdf_file}")

            # Zero-padding for consistent file naming
            page_digits = len(str(total_pages))
            for page_num in range(total_pages):
                try:
                    page = doc[page_num]
                    pix = page.get_pixmap(dpi=200)

                    output_tiff = os.path.join(
                        os.path.dirname(pdf_file),
                        f"{os.path.splitext(os.path.basename(pdf_file))[0]}_page_{str(page_num + 1).zfill(page_digits)}.tif"
                    )

                    # Convert the pixmap to an image
                    img = Image.open(io.BytesIO(pix.tobytes("ppm"))).convert("L")
                    img = img.point(lambda x: 0 if x < 128 else 255, '1')  # Binarize the image
                    img.save(output_tiff, "TIFF", compression="group4", dpi=(200, 200))

                    print(f"Saved TIFF: {output_tiff}")
                except Exception as e:
                    print(f"Error processing page {page_num + 1} of {pdf_file}: {e}")
                    continue

            doc.close()

            # Remove the original PDF after successful processing
            if os.path.exists(pdf_file):
                os.remove(pdf_file)
                print(f"Removed original PDF: {pdf_file}")

        except Exception as e:
            print(f"Error processing PDF {pdf_file}: {e}")

    def process_jpeg(self, jpeg_file):
        """Converts a JPEG file to a TIFF file and removes the original JPEG."""
        try:
            # Open and convert the JPEG to grayscale, then binarize
            img = Image.open(jpeg_file)
            img = img.convert("L")
            img = img.point(lambda x: 0 if x < 128 else 255, '1')

            output_tiff = os.path.join(os.path.dirname(jpeg_file), f"{os.path.splitext(os.path.basename(jpeg_file))[0]}.tif")
            img.save(output_tiff, "TIFF", compression="group4", dpi=(200, 200))

            print(f"Processed JPEG to TIFF: {output_tiff}")

            # Remove the original JPEG after processing
            if os.path.exists(jpeg_file):
                os.remove(jpeg_file)
                print(f"Removed original JPEG: {jpeg_file}")

        except Exception as e:
            print(f"Error processing JPEG {jpeg_file}: {e}")

if __name__ == "__main__":
    args = parse_args()
    watch_directory = args.watch_dir
    output_directory = args.output_dir

    # Ensure watch and output directories exist
    if not os.path.exists(watch_directory):
        print(f"Watch directory does not exist: {watch_directory}")
        sys.exit()

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print(f"Created output directory: {output_directory}")

    # Start observing for new folders
    event_handler = PDFJPEGHandler(output_directory, watch_directory)
    observer = Observer()
    observer.schedule(event_handler, watch_directory, recursive=True)  # Recursive to include all subdirectories
    observer.start()
    print("Observer started...")

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
        print("Observer stopped.")
    observer.join()
    print("Observer joined and exiting.")
