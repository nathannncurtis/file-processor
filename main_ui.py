import multiprocessing
import subprocess
import os
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QFileDialog, QPushButton, QListWidget, QVBoxLayout, QWidget, QLabel, QInputDialog, QSpacerItem, QSizePolicy, QSystemTrayIcon, QMenu, QAction, qApp, QDialog, QLineEdit, QSpinBox, QDialogButtonBox, QMenuBar, QMessageBox, QDesktopWidget
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QIcon, QCursor
from job_manager import JobManager
from time import sleep
import time

BASE_DIR = os.path.dirname(os.path.abspath(sys.executable)) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, 'config.json')

class JobQueueManager(QThread):
    job_finished_signal = pyqtSignal()

    def __init__(self, manager, core_cap):
        super().__init__()
        self.active_processes = []  # Store references to running processes
        self.manager = manager
        self.core_cap = core_cap
        self.active_jobs = []
        self.job_queue = []
        
    def run(self):
        while True:
            if len(self.active_jobs) < self.core_cap and self.job_queue:
                profile, processor_name, watch_dir, output_dir = self.job_queue.pop(0)
                self.start_job(profile, processor_name, watch_dir, output_dir)
            self.sleep(1)  # Polling interval

    def start_job(self, profile, processor_name, watch_dir, output_dir):
        """Start a job and keep track of the process."""
        try:
            # Determine if running as an executable
            if getattr(sys, 'frozen', False):
                # Use the .exe if running as a frozen app
                exe_dir = os.path.dirname(sys.executable)
                # Force replacement of .py with .exe
                if processor_name.endswith('.py'):
                    processor_name = os.path.splitext(processor_name)[0] + '.exe'
                processor_path = os.path.join(exe_dir, processor_name)
            else:
                # Use the .py directly if running as a script
                processor_path = processor_name

            # Start the processor as a daemon process (no terminal window)
            process = subprocess.Popen(
                [processor_path, '--watch-dir', watch_dir, '--output-dir', output_dir],
                creationflags=subprocess.CREATE_NO_WINDOW
            )

            # Store the process so it can be terminated later
            self.active_processes.append(process)

        except Exception as e:
            print(f"Failed to start job for {processor_name}: {e}")

    def stop_all_processes(self):
        """Terminate all active processes."""
        for process in self.active_processes:
            try:
                process.terminate()  # Send termination signal
                process.wait()  # Wait for the process to exit
                print(f"Terminated process: {process.pid}")
            except Exception as e:
                print(f"Failed to terminate process: {e}")
        self.active_processes.clear()  # Clear the list of active processes

    def queue_job(self, profile, processor_name, watch_dir, output_dir):
        self.job_queue.append((profile, processor_name, watch_dir, output_dir))

class CoreCapDialog(QDialog):
    def __init__(self, current_core_cap, max_cores, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Set Core Cap")
        
        layout = QVBoxLayout(self)

        instruction_label = QLabel("Enter number of cores to use:")
        instruction_label.setStyleSheet("color: black;")
        layout.addWidget(instruction_label)

        self.spin_box = QSpinBox(self)
        self.spin_box.setRange(1, max_cores)
        self.spin_box.setValue(current_core_cap)
        layout.addWidget(self.spin_box)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
        self.center_on_cursor()

    def center_on_cursor(self):
        screen = QApplication.screenAt(QCursor.pos())
        screen_geometry = screen.geometry()
        self.move(screen_geometry.center() - self.rect().center())

    def get_value(self):
        return self.spin_box.value()

class ProfileNameDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add New Job Profile")

        layout = QVBoxLayout(self)

        instruction_label = QLabel("Enter Profile Name:")
        instruction_label.setStyleSheet("color: black;")
        layout.addWidget(instruction_label)

        self.profile_input = QLineEdit(self)
        layout.addWidget(self.profile_input)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
        self.center_on_cursor()

    def center_on_cursor(self):
        screen = QApplication.screenAt(QCursor.pos())
        screen_geometry = screen.geometry()
        self.move(screen_geometry.center() - self.rect().center())

    def get_value(self):
        return self.profile_input.text()

class MainUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("File Processor")
        self.setGeometry(100, 100, 600, 400)

        self.core_count = multiprocessing.cpu_count()
        self.core_cap = self.core_count
        self.manager = JobManager(CONFIG_FILE)
        self.network_folder = self.manager.config.get('network_folder', '')

        self.init_tray()
        self.init_ui()
        self.init_menu()

        self.queue_manager = JobQueueManager(self.manager, self.core_cap)
        self.queue_manager.start()
        self.center_on_cursor()

    def init_tray(self):
        self.tray_icon = QSystemTrayIcon(self)
        icon_path = os.path.join(BASE_DIR, "processor.ico")
        self.tray_icon.setIcon(QIcon(icon_path))
        self.tray_icon.setToolTip("File Processor App")

        self.tray_menu = QMenu(self)
        show_action = QAction("Show", self)
        quit_action = QAction("Quit", self)
        self.update_profile_status_menu()

        show_action.triggered.connect(self.show)
        quit_action.triggered.connect(self.confirm_quit)

        self.tray_menu.addAction(show_action)
        self.tray_menu.addAction(quit_action)

        self.tray_icon.setContextMenu(self.tray_menu)
        self.tray_icon.show()

    def update_profile_status_menu(self):
        """Update the system tray menu to show profile names and their statuses."""
        self.tray_menu.clear()

        profiles_with_status = self.manager.get_profiles_with_status()
        for profile, details in profiles_with_status.items():
            status = details['status']
            profile_action = QAction(f"{profile} - {status}", self)
            self.tray_menu.addAction(profile_action)

        show_action = QAction("Show", self)
        quit_action = QAction("Quit", self)
        show_action.triggered.connect(self.show)
        quit_action.triggered.connect(self.confirm_quit)
        self.tray_menu.addSeparator()
        self.tray_menu.addAction(show_action)
        self.tray_menu.addAction(quit_action)

    def init_ui(self):
        icon_path = os.path.join(BASE_DIR, 'processor.ico')
        self.setWindowIcon(QIcon(icon_path))

        self.setStyleSheet("""
            QMainWindow {
                background-color: #2e2e2e;
            }
            QLabel {
                color: #ffffff;
                font-size: 14px;
            }
            QPushButton {
                background-color: #4a90e2;
                color: white;
                font-size: 14px;
                padding: 8px;
                border: none;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #357ABD;
            }
            QListWidget {
                background-color: #3e3e3e;
                color: white;
                padding: 8px;
                font-size: 13px;
                border: 1px solid #4a90e2;
                border-radius: 4px;
            }
        """)

        layout = QVBoxLayout()

        folder_label_font = QFont()
        folder_label_font.setPointSize(12)
        folder_label_font.setBold(True)

        self.folder_label = QLabel(f"Network Folder: {self.network_folder}", self)
        self.folder_label.setFont(folder_label_font)
        layout.addWidget(self.folder_label)

        set_folder_btn = QPushButton("Set Network Folder", self)
        layout.addWidget(set_folder_btn)
        set_folder_btn.clicked.connect(self.set_network_folder)

        layout.addSpacerItem(QSpacerItem(0, 20, QSizePolicy.Minimum, QSizePolicy.Fixed))

        job_profile_label = QLabel("Job Profiles:", self)
        job_profile_label.setFont(folder_label_font)
        layout.addWidget(job_profile_label)

        self.job_list = QListWidget(self)
        self.load_profiles()
        layout.addWidget(self.job_list)

        add_job_btn = QPushButton("Add Job Profile", self)
        layout.addWidget(add_job_btn)
        add_job_btn.clicked.connect(self.add_job)

        remove_job_btn = QPushButton("Remove Job Profile", self)
        layout.addWidget(remove_job_btn)
        remove_job_btn.clicked.connect(self.remove_job)

        self.core_cap_btn = QPushButton(f"Set Core Cap (Currently: {self.core_cap})", self)
        layout.addWidget(self.core_cap_btn)
        self.core_cap_btn.clicked.connect(self.set_core_cap)

        layout.addSpacerItem(QSpacerItem(0, 20, QSizePolicy.Minimum, QSizePolicy.Fixed))

        toggle_status_btn = QPushButton("Pause/Unpause Job", self)
        layout.addWidget(toggle_status_btn)
        toggle_status_btn.clicked.connect(self.toggle_job_status)

        widget = QWidget()
        widget.setLayout(layout)
        self.setCentralWidget(widget)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(10)

    def init_menu(self):
        """Initialize the file menu with a quit option."""
        menubar = self.menuBar()
        file_menu = menubar.addMenu('File')

        quit_action = QAction("Quit", self)
        quit_action.triggered.connect(self.confirm_quit)
        file_menu.addAction(quit_action)

    def confirm_quit(self):
        """Show a confirmation dialog before quitting and stop all background processes if confirmed."""
        reply_box = QMessageBox(self)
        reply_box.setWindowTitle("Quit Confirmation")
        reply_box.setText("Are you sure you want to quit?")
        reply_box.setStyleSheet("QLabel { color: black; }") 
        reply_box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        self.center_on_cursor(reply_box)
        reply = reply_box.exec_()

        if reply == QMessageBox.Yes:
            # Stop all background processes before quitting
            self.queue_manager.stop_all_processes()
            qApp.quit()

    def center_on_cursor(self, window=None):
        """Center the given window or the main window on the monitor where the cursor is."""
        if window is None:
            window = self
        screen = QApplication.screenAt(QCursor.pos())
        screen_geometry = screen.geometry()
        window.move(screen_geometry.center() - window.rect().center())

    def closeEvent(self, event):
        """Override close event to minimize to tray."""
        event.ignore()
        self.hide()
        self.tray_icon.showMessage(
            "File Processor App",
            "Application minimized to tray. Right-click the tray icon for options.",
            QSystemTrayIcon.Information,
            2000
        )

    def set_network_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Network Folder")
        if folder:
            self.network_folder = folder
            self.folder_label.setText(f"Network Folder: {self.network_folder}")
            self.manager.update_network_folder(folder)
            self.load_profiles()
            self.update_profile_status_menu()

    def load_profiles(self):
        self.job_list.clear()
        for profile, details in self.manager.get_profiles_with_status().items():
            status = details['status']
            self.job_list.addItem(f"{profile} - {status}")
        self.update_profile_status_menu()

    def add_job(self):
        dialog = ProfileNameDialog(self)
        if dialog.exec_():
            profile_name = dialog.get_value()
            if profile_name:
                self.manager.add_profile(profile_name)

                # Determine if we're running as an executable
                if getattr(sys, 'frozen', False):
                    # Running as an executable, use .exe files
                    exe_dir = os.path.dirname(sys.executable)
                    jpeg_processor_exe = os.path.join(exe_dir, "jpeg_processor.exe")
                    tiff_processor_exe = os.path.join(exe_dir, "tiff_processor.exe")
                else:
                    # Running as a script, use .py files
                    jpeg_processor_exe = "jpeg_processor.py"
                    tiff_processor_exe = "tiff_processor.py"

                # Queue the jobs using the appropriate executable or script
                self.queue_manager.queue_job(profile_name, jpeg_processor_exe, self.manager.config['profiles'][profile_name]['JPEG'], self.manager.config['profiles'][profile_name]['COMPLETE'])
                self.queue_manager.queue_job(profile_name, tiff_processor_exe, self.manager.config['profiles'][profile_name]['TIFF'], self.manager.config['profiles'][profile_name]['COMPLETE'])
                
                self.load_profiles()

    def remove_job(self):
        selected_item = self.job_list.currentItem()
        if selected_item:
            profile_name = selected_item.text().split(" - ")[0]
            self.manager.remove_profile(profile_name)
            self.load_profiles()

    def toggle_job_status(self):
        selected_item = self.job_list.currentItem()
        if selected_item:
            profile_name = selected_item.text().split(" - ")[0]
            self.manager.toggle_profile_status(profile_name)
            self.load_profiles()

    def set_core_cap(self):
        dialog = CoreCapDialog(self.core_cap, self.core_count, self)
        if dialog.exec_():
            new_cap = dialog.get_value()
            self.core_cap = new_cap
            self.queue_manager.core_cap = new_cap
            self.manager.update_core_cap(new_cap)
            self.update_core_cap_button()

    def update_core_cap_button(self):
        self.core_cap_btn.setText(f"Set Core Cap (Currently: {self.core_cap})")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainUI()
    window.show()
    sys.exit(app.exec_())
