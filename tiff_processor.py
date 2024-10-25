import os
import time
import shutil
import sys
<<<<<<< HEAD
=======
import threading
>>>>>>> b61d9cc (final version)
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import fitz  # PyMuPDF
from PIL import Image
import io
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="TIFF Processor for PDFs and JPEGs")
    parser.add_argument('--watch-dir', required=True, help='Directory to watch for new PDFs and JPEGs')
    parser.add_argument('--output-dir', required=True, help='Directory to move completed files')
    return parser.parse_args()

class PDFJPEGHandler(FileSystemEventHandler):
    def __init__(self, output_directory):
        self.output_directory = output_directory

    def on_created(self, event):
        time.sleep(10)  
        if event.is_directory:
            self.process_directory(event.src_path)
        elif event.src_path.lower().endswith(".pdf"):
<<<<<<< HEAD
            self.process_pdf(event.src_path)
        elif event.src_path.lower().endswith(".jpeg") or event.src_path.lower().endswith(".jpg"):
            self.process_jpeg(event.src_path)
=======
            threading.Thread(target=self.process_pdf, args=(event.src_path,)).start()
        elif event.src_path.lower().endswith(".jpeg") or event.src_path.lower().endswith(".jpg"):
            threading.Thread(target=self.process_jpeg, args=(event.src_path,)).start()
>>>>>>> b61d9cc (final version)

    def process_directory(self, folder_path):
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                full_path = os.path.join(root, file)
                if file.lower().endswith(".pdf"):
<<<<<<< HEAD
                    self.process_pdf(full_path)
                elif file.lower().endswith(".jpeg") or file.lower().endswith(".jpg"):
                    self.process_jpeg(full_path)
=======
                    threading.Thread(target=self.process_pdf, args=(full_path,)).start()
                elif file.lower().endswith(".jpeg") or file.lower().endswith(".jpg"):
                    threading.Thread(target=self.process_jpeg, args=(full_path,)).start()
>>>>>>> b61d9cc (final version)

        self.move_folder(folder_path)

    def move_folder(self, folder_path):
        destination_folder = self.output_directory
        try:
            shutil.move(folder_path, destination_folder)
        except Exception as e:
            print(f"Error moving folder: {e}")

    def process_pdf(self, pdf_file):
        file_ready = False
        retry_count = 0
        max_retries = 5
        stable_duration = 5

        while not file_ready and retry_count < max_retries:
            try:
                initial_size = os.path.getsize(pdf_file)
                time.sleep(stable_duration)
                current_size = os.path.getsize(pdf_file)
                if initial_size == current_size:
                    file_ready = True
                else:
                    retry_count += 1
            except FileNotFoundError:
                return

        if not file_ready:
            return

        relative_path = os.path.relpath(os.path.dirname(pdf_file), watch_directory)
        destination_folder = os.path.join(self.output_directory, relative_path)
        os.makedirs(destination_folder, exist_ok=True)

        destination_file = os.path.join(destination_folder, os.path.basename(pdf_file))
        shutil.move(pdf_file, destination_file)

        try:
            doc = fitz.open(destination_file)
            total_pages = len(doc)
            page_digits = len(str(total_pages))

            for page_num in range(total_pages):
<<<<<<< HEAD
                page = doc.load_page(page_num)
                pix = page.get_pixmap(dpi=200)

                img = Image.open(io.BytesIO(pix.tobytes("ppm")))
                img = img.convert("L")
                img = img.point(lambda x: 0 if x < 128 else 255, '1')

                output_tiff = os.path.join(destination_folder, f"{os.path.splitext(os.path.basename(pdf_file))[0]}_page_{str(page_num + 1).zfill(page_digits)}.tif")
                img.save(output_tiff, "TIFF", compression="group4", dpi=(200, 200))
=======
                threading.Thread(target=self.save_tiff, args=(doc, destination_folder, pdf_file, page_num, page_digits)).start()
>>>>>>> b61d9cc (final version)

            doc.close()
        except Exception as e:
            print(f"Error processing PDF to TIFF: {e}")

<<<<<<< HEAD
    def process_jpeg(self, jpeg_file):
        try:
=======
    def save_tiff(self, doc, destination_folder, pdf_file, page_num, page_digits):
        page = doc.load_page(page_num)
        pix = page.get_pixmap(dpi=200)

        img = Image.open(io.BytesIO(pix.tobytes("ppm")))
        img = img.convert("L")
        img = img.point(lambda x: 0 if x < 128 else 255, '1')

        output_tiff = os.path.join(destination_folder, f"{os.path.splitext(os.path.basename(pdf_file))[0]}_page_{str(page_num + 1).zfill(page_digits)}.tif")
        img.save(output_tiff, "TIFF", compression="group4", dpi=(200, 200))
        print(f"Saved TIFF: {output_tiff}")

    def process_jpeg(self, jpeg_file):
        try:
            # Open and convert JPEG to black-and-white
>>>>>>> b61d9cc (final version)
            img = Image.open(jpeg_file)
            img = img.convert("L")
            img = img.point(lambda x: 0 if x < 128 else 255, '1')

<<<<<<< HEAD
            destination_folder = os.path.join(self.output_directory, os.path.relpath(os.path.dirname(jpeg_file), watch_directory))
            os.makedirs(destination_folder, exist_ok=True)

            output_tiff = os.path.join(destination_folder, f"{os.path.splitext(os.path.basename(jpeg_file))[0]}.tif")
            img.save(output_tiff, "TIFF", compression="group4", dpi=(200, 200))
=======
            # Define the output TIFF file path
            destination_folder = os.path.join(self.output_directory, os.path.relpath(os.path.dirname(jpeg_file), watch_directory))
            os.makedirs(destination_folder, exist_ok=True)
            
            output_tiff = os.path.join(destination_folder, f"{os.path.splitext(os.path.basename(jpeg_file))[0]}.tif")
            img.save(output_tiff, "TIFF", compression="group4", dpi=(200, 200))
            print(f"Saved TIFF: {output_tiff}")

            # After successfully saving the TIFF, delete the original JPEG
            os.remove(jpeg_file)
            print(f"Original JPEG removed: {jpeg_file}")

>>>>>>> b61d9cc (final version)
        except Exception as e:
            print(f"Error processing JPEG to TIFF: {e}")

if __name__ == "__main__":
    args = parse_args()
    watch_directory = args.watch_dir
    output_directory = args.output_dir

    if not os.path.exists(watch_directory):
        exit()

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    event_handler = PDFJPEGHandler(output_directory)
    observer = Observer()
    observer.schedule(event_handler, watch_directory, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
