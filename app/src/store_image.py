import cv2
import time
import queue
import threading
from src.label import Label,Shape

class StoreImage:
    def __init__(self):
        self.imageQueue = queue.Queue(maxsize=50)
        self.thread = threading.Thread(target=self.storeimage)
        self.thread.start()
        self.label = Label()
        self.shape = Shape()

    def storeimage(self):
        try:
            while True:
                try:
                    if self.imageQueue.qsize() > 0:
                        image,location,label = self.imageQueue.get()
                        if label == True:
                            for cb in image:
                                cb[0](cb[1])
                        
                        else:
                            cv2.imwrite(location,image)

                    else:
                        time.sleep(0.01)
                except Exception as e:
                    print(e)
                    time.sleep(0.01)
                    continue
        except Exception as e:
            print(e)
            time.sleep(0.01)

    def setImage(self,image,location):##fda
        try:
            if self.imageQueue.full() != True:
               self.imageQueue.put([image,location,False])
            return True
        except Exception as e:
            print(e)
            time.sleep(0.01)

    def setDefectImage(self,image,location):##mddimage ,add
        try:
            if self.imageQueue.full() != True:
                self.imageQueue.put([image,location,False])
            return True
        except Exception as e:
            print(e)
            time.sleep(0.01)

    def setLabel(self,image,result,coordinates,location,exposure,gain, program, score, timestamp):  
        try:
            #create a callback to save the label
            self.label = Label()
            self.shape = Shape()
            cb = []
            cb.append([self.shape.add_label,result])
            # x1,y1,x2,y2 = coordinates
            # x1,y1,x2,y2 = int(x1),int(y1),int(x2),int(y2)
            cb.append([self.shape.add_points,coordinates])
            cb.append([self.shape.add_shape_type,'rectangle'])
            cb.append([self.shape.add_score, float(score)])  # Convert numpy.float32
            cb.append([self.label.add_shapes,self.shape])
            cb.append([self.label.add_exposure,exposure])
            cb.append([self.label.add_gain,gain])
            cb.append([self.label.add_program,program])
            cb.append([self.label.add_timestamp, str(timestamp)])  # Ensure timestamp is string
            
            cb.append([self.label.add_imagePath,location.split("/")[-1]])
            cb.append([self.label.setImage,image])
            location = location.rsplit('.', 1)[0] + ".json"
            cb.append([self.label.saveLabel,location])
            if self.imageQueue.full()  != True:
                self.imageQueue.put([cb,"",True])
            del(self.label)
            del(self.shape)
            return True
        except Exception as e:
            print(e)
            time.sleep(0.01)