import json
import base64
import numpy as np
import cv2

class Shape:
    """
    creating labelme format shapes
    """
    def __init__(self):
        self.shape = {}
        self.shape['label'] = ''
        self.shape['points'] = []
        self.shape['group_id'] = None
        self.shape['description'] = ""
        self.shape['shape_type'] = 'rectangle'
        self.shape['flags'] = {}
        self.shape['score'] = ''
    
    def add_label(self, label):
        self.shape['label'] = label
    
    def add_points(self, points):
        self.shape['points'] = points
    
    def add_shape_type(self, shape_type):
        self.shape['shape_type'] = shape_type

    def add_score(self, score):
        self.shape['score'] = score

    def get_shape(self):
        return self.shape




class Label:
    """
    creating labelme format labels
    """
    def __init__(self):
        self.label = {}
        self.label['version'] = '4.5.6'
        self.label['flags'] = {}
        self.label['shapes'] = []
        self.label['exposure'] = 500
        self.label['gain'] = 20
        self.label['program_name'] = ''
        self.label['gsm'] = ''
        self.label['gg'] = ''
        self.label['fabric_type'] = ''
        self.label['job_no'] = ''
        self.label['needle_drop'] = ''
        self.label['roll_weight'] = ''
        self.label['doff'] = ''
        self.label['count'] = ''
        self.label['mill'] = ''
        self.label['dia'] = ''
        self.label['lot'] = ''
        self.label['party'] = ''
        self.label['loop_length'] = ''
        self.label['knit_type'] = ''
        self.label['denier'] = ''
        self.label['timestamp'] = ''
        self.label['imageHeight'] = 0
        self.label['imageWidth'] = 0
        self.label['imagePath'] = ''
        self.label['imageData'] = None
        
    
    def add_shapes(self, shapes):
        self.label['shapes'].append(shapes.get_shape())
        return True
    
    def add_imagePath(self, imagePath):
        self.label['imagePath'] = imagePath
        return True
    
    def add_imageData(self, imageData):
        self.label['imageData'] = imageData
        return True
    
    def add_imageHeight(self, imageHeight):
        self.label['imageHeight'] = imageHeight
        return True
    
    def add_imageWidth(self, imageWidth):
        self.label['imageWidth'] = imageWidth
        return True
    
    def add_exposure(self, exposure):
        self.label['exposure'] = exposure
        return True
    
    def add_gain(self, gain):
        self.label['gain'] = gain
        return True
    
    def add_program(self, program):
        self.label['program_name'] = program[0]['program_name']
        self.label['gsm'] = program[0]['gsm']
        self.label['gg'] = program[0]['gg']
        self.label['fabric_type'] = program[0]['fabric_type']
        self.label['job_no'] = program[0]['job_no']
        self.label['needle_drop'] = program[0]['needle_drop']
        self.label['roll_weight'] = program[0]['roll_weight']
        self.label['doff'] = program[0]['doff']
        self.label['count'] = program[0]['count']
        self.label['mill'] = program[0]['mill']
        self.label['dia'] = program[0]['dia']
        self.label['lot'] = program[0]['lot']
        self.label['party'] = program[0]['party']
        self.label['loop_length'] = program[0]['loop_length']
        self.label['knit_type'] = program[0]['knit_type']
        self.label['denier'] = program[0]['denier']
        return True

    def add_timestamp(self, timestamp):
        self.label['timestamp'] = timestamp
        return True
    
    def get_label(self):
        # print(self.label)
        return json.dumps(self.label)
    
    def setImage(self,image):
        self.add_imageHeight(image.shape[0])
        self.add_imageWidth(image.shape[1])
        self.add_imageData(self.image_to_base64(image))
        return True

    def image_to_base64(self,image):
        return base64.b64encode(cv2.imencode('.jpg', image)[1]).decode()
    
    def saveLabel(self, path):
        with open(path, 'w') as f:
            f.write(self.get_label())
        return True
    

