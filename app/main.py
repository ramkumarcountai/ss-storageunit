import configparser
import logging
import redis
import os
import threading
import cv2
import numpy as np
import pickle
import time
import json
from io import BytesIO
from src.store_image import StoreImage
import datetime
from src.db import Database  

LOG_FILE = "saveimage.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class RedisListener:
    def __init__(self, redis_host, database_host, redis_channels):
        self.database = Database(database_host)
        self.REDIS_CHANNELS = redis_channels
        self.store_image = StoreImage()

    def log_restart(self, reason):
        """Logs a restart event."""
        logging.info(f"Restarting due to: {reason}")

    def listen_to_redis(self, redis_host, redis_port):
        connected_once = False  # Track if we ever successfully connected

        while True:
            try:
                print(f"Connecting to Redis at {redis_host}:{redis_port}...")
                r = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    db=0,
                    socket_keepalive=True,
                    socket_timeout=5,  
                    retry_on_timeout=True, 
                )
             
                r.ping()
                print(f"✅ Successfully connected to Redis at {redis_host}:{redis_port}")
                logging.info(f"Connected to Redis at {redis_host}:{redis_port}")
                connected_once = True  
                
                pubsub = r.pubsub()
                pubsub.subscribe(*self.REDIS_CHANNELS)
                print(f"Listening for messages on {redis_host}:{redis_port}...")

                while True: 
                    message = pubsub.get_message(timeout=1)  
                    if message:
                        if message["type"] != "message":
                            continue
                        try:
                            channel = message["channel"].decode("utf-8") if isinstance(message["channel"], bytes) else message["channel"]
                            raw_data = message["data"]
                            print(f"📩 Message received on channel: {channel}")

                            if channel == "channel1":
                                self.handle_channel1(raw_data)
                            elif channel == "mdd/image":  
                                self.handle_mdd(raw_data, redis_host)
                            elif channel == "mdd/json":
                                self.handle_mdd_json(raw_data)
                            elif channel=="add/image":
                                self.handle_add_image(raw_data,redis_host)
                            elif channel == "add/json":
                                self.handle_add_json(raw_data)
                        except Exception as e:
                            logging.error(f"⚠️ Error processing message: {e}")
                            print(f"⚠️ Error processing message: {e}")
                    
                    try:
                        r.ping()
                    except redis.ConnectionError:
                        raise redis.ConnectionError("Lost connection")

            except redis.ConnectionError as e:
                if not connected_once:
                    issue = f"❌ Failed to connect to Redis at {redis_host}:{redis_port} - {e}"
                else:
                    issue = f"🚨 Redis connection lost during runtime at {redis_host}:{redis_port} - {e}"

                print(issue)
                logging.error(issue)
                time.sleep(1)  # Wait before retrying
            except Exception as e:
                issue = f"Unexpected error at {redis_host}:{redis_port} - {e}"
                print(issue)
                logging.error(issue)


    def handle_channel1(self, raw_data):
        """ Handles messages from 'channel1'. """
        try:
            decoded_data = None

            # Check for NumPy format
            if raw_data.startswith(b'\x93NUMPY'):
                try:
                    decoded_data = np.load(BytesIO(raw_data))
                except Exception as e:
                    print(f"Error decoding NumPy data: {e}")
                    return

            # Check for Pickle format
            elif raw_data.startswith(b'\x80'):
                try:
                    decoded_data = pickle.loads(raw_data)
                except pickle.UnpicklingError as e:
                    print(f"Error decoding pickle data: {e}")
                    return

            # Check for JSON format
            elif raw_data.startswith(b'{'):
                try:
                    decoded_data = json.loads(raw_data.decode("utf-8"))
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON data: {e}")
                    return

            if not isinstance(decoded_data, dict):
                print("Decoded data is not a valid dictionary, skipping...")
                return

            # Ensure required keys exist
            if "imageId" not in decoded_data or "filepath" not in decoded_data:
                print("Missing required keys (imageId, filepath), skipping...")
                return

            # Extract and process image ID
            image_id = decoded_data["imageId"]
            if image_id.startswith("1_"):
                image_id = image_id[2:]

            # Extract and format file path
            filepath = decoded_data["filepath"]
            full_local_path = os.path.join("/home/kniti/projects/knit-i/knitting-core", filepath.lstrip("./"))

            # Ensure the directory exists
            os.makedirs(os.path.dirname(full_local_path), exist_ok=True)

            # Retrieve the image data
            image_data = decoded_data.get("image")
            if image_data is None:
                print("No image data found, skipping...")
                return

            # Decode the image if it's in ndarray format or raw byte format
            if isinstance(image_data, np.ndarray):
                image = image_data
            else:
                try:
                    nparr = np.frombuffer(image_data, np.uint8)
                    image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                    if image is None or image.size == 0:
                        raise ValueError("Decoded image is empty or invalid")
                except Exception as e:
                    print(f"Error decoding image: {e}")
                    return

            # Resize image to 1280x720
            try:
                image = cv2.resize(image, (1280, 720), interpolation=cv2.INTER_AREA)
            except Exception as e:
                print(f"Error resizing image: {e}")
                return

            # Save the resized image
            filename = os.path.join(full_local_path, image_id)
            self.store_image.setImage(image, filename)
            print(f"Image saved successfully at: {filename}")

        except Exception as e:
            print(f"Unexpected error in handle_channel1: {e}")

            
    def handle_mdd(self, raw_data, redis_host):
        try:
            print(f"? Received raw data (first 50 bytes): {raw_data[:50]}")
            decoded_data = pickle.loads(raw_data)
            print(f"? Successfully decoded pickle data from 'mdd/image' on {redis_host}")

            if not isinstance(decoded_data, (list, tuple)) or len(decoded_data) != 2:
                print("?? Unexpected data structure in 'mdd/image', skipping...")
                return

            numpy_array, image_path = decoded_data

            if not isinstance(numpy_array, np.ndarray) or not isinstance(image_path, str):
                print("?? Invalid data format! Expected (NumPy array, image path). Skipping...")
                return

            # Resize image to 1280x720
            resized_image = cv2.resize(numpy_array, (1280, 720))

            # Define base directory
            base_dir = "/home/kniti/projects/knit-i/knitting-core"

            # Ensure image_path is relative and construct full path
            full_image_path = os.path.join(base_dir, image_path.lstrip("/"))

            # Ensure the directory exists before saving
            os.makedirs(os.path.dirname(full_image_path), exist_ok=True)

            # Call store_image.setImage function with resized image
            self.store_image.setImage(resized_image, full_image_path)

            print(f" Image saved successfully at: {full_image_path}")

        except Exception as e:
            print(f"? Error in handle_mdd: {e}")
    
    def handle_add_image(self, raw_data, redis_host):
        """Handles messages from 'add/image' and saves the image to a different path."""
        try:
            print(f"? Received raw data (first 50 bytes): {raw_data[:50]}")

            decoded_data = pickle.loads(raw_data)
            print(f"? Successfully decoded pickle data from 'add/image' on {redis_host}")

            # Print the full structure of decoded_data
            print(f"? Decoded Data Type: {type(decoded_data)}")
            print(f"? Decoded Data Content: {decoded_data}")

            if not isinstance(decoded_data, (list, tuple)) or len(decoded_data) != 2:
                print(f"?? Unexpected data structure in 'add/image', skipping...")
                return

            numpy_array, image_path = decoded_data

            if not isinstance(numpy_array, np.ndarray) or not isinstance(image_path, str):
                print(f"?? Invalid data format! Expected (NumPy array, image path). Skipping...")
                return

            resized_image = cv2.resize(numpy_array, (1280, 720))

            # Define base directory
            base_dir = "/home/kniti/projects/knit-i/knitting-core"

            # Ensure image_path is relative and construct full path
            full_image_path = os.path.join(base_dir, image_path.lstrip("/"))

            # Ensure the directory exists before saving
            os.makedirs(os.path.dirname(full_image_path), exist_ok=True)

            # Call store_image.setImage function
            self.store_image.setImage(resized_image, full_image_path)

            print(f" Image saved successfully at: {full_image_path}")

        except Exception as e:
            print(f" Error in handle_add_image: {e}")
    
    
    
    def handle_mdd_json(self, raw_data):
        """Handles messages from 'mdd/json'."""
        try:
            print(f"Received raw data from 'mdd/json' (first 50 bytes): {raw_data[:50]}")

            # Unpickle the data
            decoded_data = pickle.loads(raw_data)
            print(f"Successfully decoded data from 'mdd/json': {decoded_data}")

            # Ensure data is a list with at least 7 elements (based on your example structure)
            if not isinstance(decoded_data, list) or len(decoded_data) < 7:
                print("Invalid data structure: Expected a list with at least 7 elements.")
                return

            # Extract elements
            image_data = decoded_data[0]  # Image (NumPy array)
            result = decoded_data[1]  # Defect type (e.g., 'shutoff')
            coordinates = decoded_data[2]  # Defect coordinates
            location = decoded_data[3]  # File path
            exposure = decoded_data[4]  # Additional machine details (list of dicts)
            gain = decoded_data[5]
            program = decoded_data[6]
            score = float(decoded_data[7])  # Convert numpy.float32 to Python float
            timestamp = decoded_data[8]  # Timestamp (string or datetime)

            # Extract filename from the full path
            image_id = os.path.basename(location)

            # Construct full local path
            full_local_path = os.path.join("/home/kniti/projects/knit-i/knitting-core", location.strip("/"))
            directory = os.path.dirname(full_local_path)
            os.makedirs(directory, exist_ok=True)

            # Process the image
            if isinstance(image_data, np.ndarray):
                image = image_data  # It's already a NumPy array
            else:
                try:
                    nparr = np.frombuffer(image_data, np.uint8)
                    if nparr.size == 0:
                        print("Error: Empty image data buffer.")
                        return
                    image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                except Exception as e:
                    print(f"Error decoding image: {e}")
                    return

            if image is None or image.size == 0:
                print("Invalid image data, skipping...")
                return

            # Save the image using StoreImage
            print(f"Creating directory: {directory}")
            os.makedirs(directory, exist_ok=True)

            self.store_image.setLabel(image, result, coordinates, full_local_path, exposure, gain, program, score, timestamp)
            print("Image saved successfully")

            # Print extracted details
            print(f"Defect type: {result}")
            print(f"Coordinates: {coordinates}")
            print(f"Machine details: {program}")
            print(f"Score: {score}")
            print(f"Timestamp: {timestamp}")

        except Exception as e:
            print(f"Error in handle_mdd_json: {e}")

        
    def handle_add_json(self,raw_data):
        """Handles messages from 'add/json' and processes them similarly to 'mdd/json'."""
        try:
            print(f"Received raw data from 'add/json' (first 50 bytes): {raw_data[:50]}")

            # Unpickle the data
            decoded_data = pickle.loads(raw_data)
            print(f"Successfully decoded data from 'add/json': {decoded_data}")

            # Ensure data is a list with at least 7 elements (based on your example structure)
            if not isinstance(decoded_data, list) or len(decoded_data) < 7:
                print("Invalid data structure: Expected a list with at least 7 elements.")
                return

            # Extract elements
            image_data = decoded_data[0]  # Image (NumPy array)
            result = decoded_data[1]  # Defect type (e.g., 'shutoff')
            coordinates = decoded_data[2]  # Defect coordinates
            location = decoded_data[3]  # File path
            exposure = decoded_data[4]  # Additional machine details (list of dicts)
            gain=decoded_data[5]
            program=decoded_data[6]
            score = float(decoded_data[7])  # Convert numpy.float32 to Python float
            timestamp = decoded_data[8]  # Timestamp (string or datetime)
            
            # Extract filename from the full path
            image_id = os.path.basename(location)  # e.g., 'CAI_101_110_117_greencam1_2025-02-18_12_16_04-495042_100_image.jpg'

            # Construct full local path
            full_local_path = os.path.join("/home/kniti/projects/knit-i/knitting-core", location.strip("/"))
            # full_local_path=full_local_path.replace(".jpg",".json")
            directory = os.path.dirname(full_local_path)
            os.makedirs(directory, exist_ok=True)

            # Process the image
            if isinstance(image_data, np.ndarray):
                image = image_data  # It's already a NumPy array
            else:
                try:
                    nparr = np.frombuffer(image_data, np.uint8)
                    if nparr.size == 0:
                        print("Error: Empty image data buffer.")
                        return
                    image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                except Exception as e:
                    print(f"Error decoding image: {e}")
                    return

            if image is None or image.size == 0:
                print("Invalid image data, skipping...")
                return

            # Save the image using StoreImage (or similar function)
            directory = os.path.dirname(full_local_path)
            print(f"Creating directory: {directory}")
            os.makedirs(directory, exist_ok=True)

            self.store_image.setLabel(image,result,coordinates,full_local_path,exposure,gain, program, score, timestamp)
            print(f"Image saved successfully ")
            # Further processing of the additional data, if needed (e.g., machine details)
            # Example: storing machine details, or using the defect type and coordinates
            print(f"Defect type: {result}")
            print(f"Coordinates: {coordinates}")
            print(f"Machine details: {program}")
            print(f"Score: {score}")
            print(f"Timestamp: {timestamp}")

        except Exception as e:
            print(f"Error in handle_add_json: {e}")

    
    def start(self,redis_host):
        thread = threading.Thread(target=self.listen_to_redis, args=(redis_host, "6379"))
        thread.daemon = True  # Allows threads to exit when the main program exits
        thread.start()
        


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("saveimage_config.ini")
    
    # Get Redis and Database host configurations
    redis_host = config.get('Redis', 'host')
    database_host = config.get('Database', 'host')
    redis_channels = config.get('Redis', 'channels').split(',')
    listener = RedisListener(redis_host, database_host, redis_channels)
    listener.start(redis_host)
