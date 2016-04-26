#import general modules
import io, os.path, time, math, threading, socket, sys
import ConfigParser, ast

#custom consumer and carbon/graphite-interface modules
import avro.schema, avro.io
from carboniface import CarbonIface
from kafka import KafkaConsumer

class PythonConsumerThread(object):
    """ Threading example class
    The run() method will be started and it will run in the background
    until the application exits.
    """

    def __init__(self):
        self.cancelled = False
        self.read_config()
        self.carboniface_loader()
        #spin up the thread
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution
        print self, 'created'

    def run(self):
        """ Method that runs forever """
        while not self.cancelled:
            self.consume_messages()
            time.sleep(1/self.fps)
        else: 
            print 'Consumer Thread Terminated'
    
    def cancel(self):
        """End this timer thread"""
        self.cancelled = True
    
    def read_config(self):
        config_directory=(os.path.abspath(os.path.join(os.path.dirname(''), '..', 'conf')) + '/')
        config = ConfigParser.RawConfigParser()
        config_file='rpi_consumer.cfg'
        config.read(config_directory+config_file)
        self.host = str(config.get('MSG','host'))
        self.port = int(config.get('MSG','port'))
        self.event_url = str(config.get('MSG','event_url'))
        self.topic=str(config.get('MSG','topic'))
        self.input_kafka_topic=str(config.get('MSG','input_kafka_topic'))
        self.bootstrap_servers=ast.literal_eval(config.get('MSG','bootstrap_servers'))
        self.group_id=str(config.get('MSG','group_id'))
        self.fps=float(config.get('MSG','fps'))
        #get schema
        self.schema_file="SensorEvent.avsc"
        self.schema = avro.schema.parse(open(config_directory+self.schema_file).read())
        print 'Avro Schema:\n',self.schema
        print

    def carboniface_loader(self):
        #information for carbon interface class
        self.data = []
        self.carbon = CarbonIface(host=self.host, port=self.port, event_url=self.event_url)

    def send_avro_consumer_message_to_carbon(self,datum,topic):
        topic += '.'+str(int(datum['sensorid'])) 
        #data is a message ala: "test.trains.3.sensor.AUDIO 7.03 1412413..."
        self.data.append((topic+'.AUDIO', (datum['timestamp_audio']/1000.0,datum['magnitude_audio'] )))
        #convert the magnitue_video to video
        magnitude_video= ast.literal_eval( datum['magnitude_video'] )
        #Send the ROIs to graphite
        for i in range(0,3):
            self.data.append((topic+'.VIDEO'+'.'+str(i),
                (datum['timestamp_audio']/1000.0, 
                    #rescale the video signal by 100/uint8(max) i.e. 255 
                    ((magnitude_video[i])*100.0/255.0))))
        if datum['train_detected'] == 'true' or datum['train_detected']==True: #TODO: Figure out if message comes in as true or True
            self.data.append((topic+'.COMBINED', (datum['timestamp_audio']/1000.0,100)))
            if datum['direction'] == 'NORTH':
                self.data.append((topic+'.COMBINED.NORTH', (datum['timestamp_audio']/1000.0,100)))
            elif datum['direction'] == 'SOUTH':
                self.data.append((topic+'.COMBINED.SOUTH', (datum['timestamp_audio']/1000.0,-100)))
        else:
            self.data.append((topic+'.COMBINED', (datum['timestamp_audio']/1000.0,0)))
            self.data.append((topic+'.COMBINED.NORTH', (datum['timestamp_audio']/1000.0,0)))
            self.data.append((topic+'.COMBINED.SOUTH', (datum['timestamp_audio']/1000.0,0)))
        #else:
        #    self.data.append((topic+'.COMBINED', (datum['timestamp_audio']/1000.0,0)))
        #send the data to carbon/graphite
        self.carbon.send_data(self.data)
        
    def consume_messages(self):
        #from:https://gist.github.com/ChristianKniep/9580204
        consumer = KafkaConsumer(self.input_kafka_topic,
                                 group_id=self.group_id,
                                 bootstrap_servers=self.bootstrap_servers)
        #From: http://stackoverflow.com/questions/31047163/with-bottledwater-pg-how-to-read-data-by-a-python-consumer/31085531#31085531
        single_message_sent=False
        for msg in consumer:
            if self.cancelled!=True:
                try:
                    self.decode_message(msg)
                except:
                    print "Unexpected error:", sys.exc_info()[0]
                    print 'Message not able to be sent:',msg
                    single_message_sent=False
                #send a message the first time this loop is run
                if single_message_sent!=True:
                    print 'Messages are now being sent to a Carbon-Whisper-Graphite server'
                    single_message_sent=True
            else:
                print 'exiting consumer loop'
                break

    def decode_message(self,msg):
        self.data=[]#reset every time for now
        value = bytearray(msg.value)
        bytes_reader = io.BytesIO(value[5:])
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.schema)
        datum = reader.read(decoder)
        #call function to send the datum to carbon/graphite (each time send the old base topic)
        self.send_avro_consumer_message_to_carbon(datum,self.topic)
        #print data