import time
import psycopg2
import traceback



# tz = pytz.timezone("Asia/Kolkata")


class Execute:
    def __init__(self, host):
        self.keepalive_kwargs = {
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 5,
            "keepalives_count": 5,
        }
        self.host = host
        self.conn = self.connect()

    def connect(self):
        while True:
            try:
                conn = psycopg2.connect(
                    database="knitting",
                    user="postgres",
                    password="55555",
                    host=self.host,
                    port="5432",
                    **self.keepalive_kwargs
                )
                print("Connected to database ", self.host)
                conn.autocommit = True
                return conn
            except Exception as e:
                print(str(e))
                traceback.print_exc()
                time.sleep(1)

    def insert(self, query):
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            cur.close()
            return True
        except Exception as e:
            #print(str(e))
            #print(traceback.format_exc())
            self.conn = self.connect()
            return False

    def insertReturnId(self, query):
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            id = cur.fetchone()[0]
            cur.close()
            return id
        except Exception as e:
            #print(str(e))
            #print(traceback.format_exc())
            self.conn = self.connect()
            return False

    def select(self, query):
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            rows = [
                dict((cur.description[i][0], value) for i, value in enumerate(row))
                for row in cur.fetchall()
            ]
            cur.close()
            return rows

        except Exception as e:
            #print(str(e))
            #print(traceback.format_exc())
            self.conn = self.connect()
            return False

    def update(self, query):
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            cur.close()
            return True
        except Exception as e:
            #print(str(e))
            #print(traceback.format_exc())
            self.conn = self.connect()
            return False

    def delete(self, query):
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            cur.close()
            return True
        except Exception as e:
            #print(str(e))
            #print(traceback.format_exc())
            self.conn = self.connect()
            return False

class  Database:

    def __init__(self, host):
        try:
            self.execute = Execute(host)
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def get_activecameras(self):
        try:
            query = """SELECT cam_name,cam_ip FROM public.cam_details WHERE camsts_id = '1'"""
            data = self.execute.select(query)
            if data:
                return data
            return []
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return []