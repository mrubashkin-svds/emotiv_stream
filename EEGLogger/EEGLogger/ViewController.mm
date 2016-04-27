//
//  ViewController.m
//  MotionDataLogger
//
//  Created by emotiv on 4/22/15.
//  Copyright (c) 2015 emotiv. All rights reserved.
//

#import "ViewController.h"
#import <edk/Iedk.h>
#import <edk/IEegData.h>

/*****************************************************/

/*If headset Insight only 5 channel AF3, AF4, T7, T8, Pz = O1 have value. 
               Another channels EEG is zero */

/*****************************************************/

IEE_DataChannel_t targetChannelList[] = {
    IED_COUNTER,
    IED_AF3, IED_F7, IED_F3, IED_FC5, IED_T7, IED_P7, IED_O1, IED_O2,
    IED_P8, IED_T8, IED_FC6, IED_F4, IED_F8, IED_AF4, IED_GYROX,IED_GYROY,
    IED_TIMESTAMP, IED_FUNC_ID, IED_FUNC_VALUE, IED_MARKER, IED_SYNC_SIGNAL
};

BOOL isConnected = NO;
const char *headerStr_Insight = "COUNTER, AF3, F7, F3, FC5, T7, P7, O1, O2, P8, T8, FC6, F4, F8, AF4, GYROX, GYROY, TIMESTAMP, FUNC_ID, FUNC_VALUE, MARKER, SYNC_SIGNAL";

const char *newLine = "\n";
const char *comma = ",";

@implementation ViewController

EmoEngineEventHandle eEvent;
EmoStateHandle eState;
DataHandle hData;

unsigned int userID					= 0;
float secs							= 1;
bool readytocollect					= false;
int state                           = 0;

NSFileHandle *file;
NSMutableData *data;

- (void)viewDidLoad {
    [super viewDidLoad];

    eEvent	= IEE_EmoEngineEventCreate();
    eState	= IEE_EmoStateCreate();
    hData   = IEE_DataCreate();
    
    IEE_EmoInitDevice();
    
    if( IEE_EngineConnect() != EDK_OK ) {
        self.labelStatus.stringValue = @"Can't connect engine";
    }
    
    NSString* fileName = @"EEGDataLogger.csv";
    NSString* createFile = @"";
    [createFile writeToFile:fileName atomically:YES encoding:NSUnicodeStringEncoding error:nil];
    
    file = [NSFileHandle fileHandleForUpdatingAtPath:fileName];
    [self saveStr:file data:data value:headerStr_Insight];
    [self saveStr:file data:data value:newLine];
    
    IEE_DataSetBufferSizeInSec(secs);
    
    [NSTimer scheduledTimerWithTimeInterval:0.01 target:self selector:@selector(getNextEvent) userInfo:nil repeats:YES];
    
    // Do any additional setup after loading the view.
}

-(void) getNextEvent {
    /*Connect with Insight headset in mode Bluetooth*/
    int numberDevice = IEE_GetInsightDeviceCount();
    if(numberDevice > 0 && !isConnected) {
        IEE_ConnectInsightDevice(0);
        isConnected = YES;
    }
    /************************************************/
        /*Connect with Epoc Plus headset in mode Bluetooth*/
//        int numberDevice = IEE_GetEpocPlusDeviceCount();
//        if(numberDevice > 0 && !isConnected) {
//            IEE_ConnectEpocPlusDevice(0);
//            isConnected = YES;
//        }
    /************************************************/
    else isConnected = NO;
    int state = IEE_EngineGetNextEvent(eEvent);
    unsigned int userID = 0;
    
    if (state == EDK_OK)
    {
        
        IEE_Event_t eventType = IEE_EmoEngineEventGetType(eEvent);
        IEE_EmoEngineEventGetUserId(eEvent, &userID);
        
        // Log the EmoState if it has been updated
        if (eventType == IEE_UserAdded)
        {
            NSLog(@"User Added");
            self.labelStatus.stringValue = @"Connected";
            IEE_DataAcquisitionEnable(userID, true);
            readytocollect = TRUE;
        }
        else if (eventType == IEE_UserRemoved)
        {
            NSLog(@"User Removed");
            self.labelStatus.stringValue = @"Disconnected";
            readytocollect = FALSE;
        }
        else if (eventType == IEE_EmoStateUpdated)
        {
            
        }
    }
    if (readytocollect)
    {
        IEE_DataUpdateHandle(0, hData);
        
        NSDate *dateCreated;
        dateCreated = [[NSDate alloc] init];
        
        unsigned int nSamplesTaken=0;
        IEE_DataGetNumberOfSample(hData,&nSamplesTaken);
        
        NSLog(@"Updated : %i",nSamplesTaken);
        if (nSamplesTaken != 0)
        {
            
            double* ddata = new double[nSamplesTaken];
            for (int sampleIdx=0 ; sampleIdx<(int)nSamplesTaken ; ++sampleIdx) {
                for (int i = 0 ; i<sizeof(targetChannelList)/sizeof(IEE_DataChannel_t) ; i++) {
                    IEE_DataGet(hData, targetChannelList[i], ddata, nSamplesTaken);
                    
                    // ddata 
                    
                    [self saveDoubleVal:file data:data value:ddata[sampleIdx]];
                    [self saveStr:file data:data value:comma];
                }
                [self saveDoubleVal:file data:data value:dateCreated.timeIntervalSince1970];
                [self saveStr:file data:data value:newLine];
            }
            delete[] ddata;
        }
    }
}

- (void)setRepresentedObject:(id)representedObject {
    [super setRepresentedObject:representedObject];

    // Update the view, if already loaded.
}

-(void) saveStr : (NSFileHandle * )file data : (NSMutableData *) data value : (const char*) str
{
    [file seekToEndOfFile];
    data = [NSMutableData dataWithBytes:str length:strlen(str)];
    [file writeData:data];
}

-(void) saveDoubleVal : (NSFileHandle * )file data : (NSMutableData *) data value : (const double) val
{
    NSString* str = [NSString stringWithFormat:@"%f",val];
    const char* myValStr = (const char*)[str UTF8String];
    [self saveStr:file data:data value:myValStr];
}

@end
