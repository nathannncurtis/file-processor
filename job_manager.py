import os
import sys
import json
import shutil
import subprocess
<<<<<<< HEAD
import multiprocessing
=======
>>>>>>> b61d9cc (final version)

class JobManager:
    def __init__(self, config_file):
        """Initialize the JobManager with a path to the config file."""
        self.config_file = config_file
        self.processes = {}  # Keep track of running processes (one per job)
        self.load_config()

    def load_config(self):
        """Loads the configuration from the JSON file."""
        if not os.path.exists(self.config_file):
            # If the config file doesn't exist, create a blank template
<<<<<<< HEAD
            self.config = {"network_folder": "", "profiles": {}, "core_cap": multiprocessing.cpu_count()}
=======
            self.config = {"network_folder": "", "profiles": {}}
>>>>>>> b61d9cc (final version)
            self.save_config()
        else:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)

    def save_config(self):
        """Saves the current configuration to the JSON file."""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=4)

<<<<<<< HEAD
    def update_core_cap(self, new_core_cap):
        """Updates the core cap in the config file."""
        self.config['core_cap'] = new_core_cap
        self.save_config()

    def get_core_cap(self):
        """Returns the current core cap from the config file."""
        return self.config.get('core_cap', multiprocessing.cpu_count())

=======
>>>>>>> b61d9cc (final version)
    def update_network_folder(self, folder):
        """Updates the top-level network folder in the config."""
        self.config['network_folder'] = folder
        self.save_config()

    def get_profiles_with_status(self):
        """Returns a dictionary of profiles with their status (active/paused)."""
        return self.config.get('profiles', {})

    def add_profile(self, profile_name):
        """Adds a new job profile, creates necessary directories, and starts the processors."""
        base_folder = os.path.join(self.config['network_folder'], profile_name)
        jpeg_folder = os.path.join(base_folder, "JPEG")
        tiff_folder = os.path.join(base_folder, "TIFF")
        complete_folder = os.path.join(base_folder, "COMPLETE")

        # Create the required directories for the profile
        os.makedirs(jpeg_folder, exist_ok=True)
        os.makedirs(tiff_folder, exist_ok=True)
        os.makedirs(complete_folder, exist_ok=True)

        # Add the profile to the configuration
        self.config['profiles'][profile_name] = {
            "JPEG": jpeg_folder,
            "TIFF": tiff_folder,
            "COMPLETE": complete_folder,
            "status": "Active"
        }
        self.save_config()

        # Start the processors when adding a new job
        self.start_processor(profile_name, "jpeg_processor.py", jpeg_folder, complete_folder)
        self.start_processor(profile_name, "tiff_processor.py", tiff_folder, complete_folder)

    def remove_profile(self, profile_name):
        """Removes a job profile and its corresponding directories."""
        base_folder = os.path.join(self.config['network_folder'], profile_name)
<<<<<<< HEAD

        # Stop any running processors for this profile before removing
        self.stop_processor(profile_name)

        # Attempt to remove the profile's directory
        try:
            shutil.rmtree(base_folder)
            print(f"Successfully removed profile folder: {base_folder}")
        except OSError as e:
            print(f"Error removing profile folder: {e}")
=======
        if os.path.exists(base_folder):
            shutil.rmtree(base_folder)
>>>>>>> b61d9cc (final version)

        # Remove the profile from the configuration
        if profile_name in self.config['profiles']:
            del self.config['profiles'][profile_name]
            self.save_config()

<<<<<<< HEAD
=======
        # Stop any running processors for this profile
        self.stop_processor(profile_name)

>>>>>>> b61d9cc (final version)
    def pause_profile(self, profile_name):
        """Marks a profile as paused in the configuration and stops the processors."""
        if profile_name in self.config['profiles']:
            self.config['profiles'][profile_name]['status'] = "Paused"
            self.save_config()
            self.stop_processor(profile_name)

    def unpause_profile(self, profile_name):
        """Marks a profile as active in the configuration and restarts the processors."""
        if profile_name in self.config['profiles']:
            self.config['profiles'][profile_name]['status'] = "Active"
            self.save_config()

            # Restart the processors
            profile = self.config['profiles'][profile_name]
            self.start_processor(profile_name, "jpeg_processor.py", profile["JPEG"], profile["COMPLETE"])
            self.start_processor(profile_name, "tiff_processor.py", profile["TIFF"], profile["COMPLETE"])

    def toggle_profile_status(self, profile_name):
        """Toggles the profile status between active and paused."""
        if profile_name in self.config['profiles']:
            current_status = self.config['profiles'][profile_name]['status']
            if current_status == "Active":
                self.pause_profile(profile_name)
            else:
                self.unpause_profile(profile_name)

    def start_processor(self, profile_name, processor_name, watch_dir, output_dir):
<<<<<<< HEAD
        """Start a processor (JPEG/TIFF) using the .py file."""
=======
        """Start a processor (JPEG/TIFF) using the .py file as a daemon."""
>>>>>>> b61d9cc (final version)
        if profile_name not in self.processes:
            self.processes[profile_name] = {}

        log_file = f"{profile_name}_{processor_name}.log"
        with open(log_file, "w") as log:
            try:
                process = subprocess.Popen(
                    ['python', processor_name, '--watch-dir', watch_dir, '--output-dir', output_dir],
                    stdout=log,
<<<<<<< HEAD
                    stderr=log
                )
                print(f"Started {processor_name} for profile {profile_name}, log output in {log_file}")
                self.processes[profile_name][processor_name] = process
                return process  # Returning the process
            except Exception as e:
                print(f"Error starting {processor_name} for profile {profile_name}: {e}")
                return None  # Ensure we return None in case of error
=======
                    stderr=log,
                    start_new_session=True  # Ensures the process runs in the background
                )
                print(f"Started {processor_name} for profile {profile_name}, log output in {log_file}")
                self.processes[profile_name][processor_name] = process
            except Exception as e:
                print(f"Error starting {processor_name} for profile {profile_name}: {e}")
>>>>>>> b61d9cc (final version)

    def stop_processor(self, profile_name):
        """Stop any running processors for the given profile."""
        if profile_name in self.processes:
            for processor_name, process in self.processes[profile_name].items():
                process.terminate()
                print(f"Terminated {processor_name} for profile {profile_name}")
            del self.processes[profile_name]
