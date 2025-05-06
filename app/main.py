import configparser
import logging
import redis
import os
import threading
import queue
import cv2
import numpy as np
import pickle
import time
import json
from io import BytesIO
from src.store_image import StoreImage
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
        self.redis_host = redis_host
        self.store_image = StoreImage()
        self.running = True  
        self.queues = {channel: queue.Queue(maxsize=500) for channel in self.REDIS_CHANNELS}

    def log_restart(self, reason):
        logging.info(f"Restarting due to: {reason}")

    def listen_to_redis(self, redis_host, redis_port):
        while self.running:
            try:
                print(f"Connecting to Redis at {redis_host}:{redis_port}...")
                r = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    db=0,
                    socket_keepalive=True,
                    socket_timeout=None,
                    retry_on_timeout=True,
                )
                r.ping()
                logging.info(f"✅ Connected to Redis at {redis_host}:{redis_port}")
                pubsub = r.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(*self.REDIS_CHANNELS)

                print(f" Subscribed to channels: {self.REDIS_CHANNELS}")

                while self.running:
                    message = pubsub.get_message(timeout=1)
                   
                    if message:
                        channel = message["channel"].decode("utf-8") if isinstance(message["channel"], bytes) else message["channel"]
                        raw_data = message["data"]

                        if channel in self.queues:
                            q = self.queues[channel]
                            if q.qsize() >= int(q.maxsize * 0.8):
                                warning_msg = f"⚠️ Queue for channel '{channel}' is {q.qsize()}/{q.maxsize} full."
                                logging.warning(warning_msg)
                            try:
                                q.put(raw_data, timeout=1)
                                print(f" Message queued for channel: {channel}")
                            except queue.Full:
                                logging.error(f"❌ Queue full for channel '{channel}', dropping message.")
                        else:
                            logging.warning(f"⚠️ Received message for unknown channel: {channel}")

            except redis.ConnectionError as e:
                logging.critical(f"🚨 Redis connection error at {redis_host}:{redis_port} — {e}")

            except Exception as e:
                logging.error(f"❗ Unexpected error in Redis listener — {e}")

    def process_channel(self, channel_name, handler_function):
        """ Generic method to process a channel """
        while self.running:
            try:
                raw_data = self.queues[channel_name].get()
                if raw_data:
                    handler_function(raw_data)
                self.queues[channel_name].task_done()
            except Exception as e:
                logging.error(f"Error processing channel '{channel_name}': {e}")

    def handle_channel1(self, raw_data):
            try:
                decoded_data = None

                if raw_data.startswith(b'\x93NUMPY'):
                    decoded_data = np.load(BytesIO(raw_data))
                elif raw_data.startswith(b'\x80'):
                    decoded_data = pickle.loads(raw_data)
                elif raw_data.startswith(b'{'):
                    decoded_data = json.loads(raw_data.decode("utf-8"))

                if not isinstance(decoded_data, dict):
                    print("Decoded data is not a valid dictionary, skipping...")
                    return

                if "imageId" not in decoded_data or "filepath" not in decoded_data:
                    print("Missing required keys (imageId, filepath), skipping...")
                    return

                image_id = decoded_data["imageId"]
                if image_id.startswith("1_"):
                    image_id = image_id[2:]

                filepath = decoded_data["filepath"]
                full_local_path = os.path.join("/home/kniti/projects/knit-i/knitting-core", filepath.lstrip("./"))
                os.makedirs(os.path.dirname(full_local_path), exist_ok=True)

                image_data = decoded_data.get("image")
                if image_data is None:
                    print("No image data found, skipping...")
                    return

                if isinstance(image_data, np.ndarray):
                    image = image_data
                else:
                    nparr = np.frombuffer(image_data, np.uint8)
                    image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                    if image is None or image.size == 0:
                        print("Error decoding image.")
                        return
               
                image = cv2.resize(image, (1280, 720), interpolation=cv2.INTER_AREA)
                filename = os.path.join(full_local_path, image_id)
                self.store_image.setImage(image, filename)
                print(f"✅ Image saved successfully at: {filename}")

            except Exception as e:
                logging.error(f"Unexpected error in handle_channel1: {e}")

           
    def handle_mdd(self, raw_data):
        try:
            decoded_data = pickle.loads(raw_data)

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
   
    def handle_mdd_json(self, raw_data):
        """Handles messages from 'mdd/json'."""
        try:
            # print(f"Received raw data from 'mdd/json' (first 50 bytes): {raw_data[:50]}")

            # Unpickle the data
            decoded_data = pickle.loads(raw_data)
            # print(f"Successfully decoded data from 'mdd/json': {decoded_data}")

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
            # print(f"Creating directory: {directory}")
            os.makedirs(directory, exist_ok=True)
            self.store_image.setLabel(image, result, coordinates, full_local_path, exposure, gain, program, score, timestamp)
            print("Image saved successfully")
         

        except Exception as e:
            print(f"Error in handle_mdd_json: {e}")

    def start(self):
        # Start listener thread
        listener_thread = threading.Thread(target=self.listen_to_redis, args=(self.redis_host, 6379))
        listener_thread.daemon = True
        listener_thread.start()

        # Map channels to their respective processing functions
        channel_processor_map = {
            "channel1": self.handle_channel1,
            "mdd/image": self.handle_mdd,
            "mdd/json": self.handle_mdd_json,
        }

       # Start processor threads dynamically
        for channel, handler_function in channel_processor_map.items():
            if channel in self.REDIS_CHANNELS:
                if channel == "channel1":
                    thread_count = 5
                elif channel in ("mdd/json", "mdd/image"):
                    thread_count = 4
                else:
                    thread_count = 1

                for i in range(thread_count):
                    processor_thread = threading.Thread(
                        target=self.process_channel,
                        args=(channel, handler_function),
                        name=f"{channel}-processor-{i+1}"
                    )
                    processor_thread.daemon = True
                    processor_thread.start()

   
       
if __name__ == "__main__":
    if not os.path.exists("saveimage_config.ini"):
        raise FileNotFoundError("Config file saveimage_config.ini not found!")

    config = configparser.ConfigParser()
    config.read("saveimage_config.ini")

    try:
        redis_host = config.get('Redis', 'host')
        database_host = config.get('Database', 'host')
        redis_channels = config.get('Redis', 'channels').split(',')
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        raise RuntimeError(f"Invalid config file format: {e}")

    listener = RedisListener(redis_host, database_host, redis_channels)
    listener.start()
