//
//  ViewController.h
//  EEGLogger
//
//  Created by emotiv on 4/22/15.
//  Copyright (c) 2015 emotiv. All rights reserved.
//

#import <Cocoa/Cocoa.h>

@interface ViewController : NSViewController {
        NSArray        *name_channel;
        NSString       *documentDirectory   ;
}

@property (weak) IBOutlet NSTextField *labelStatus;

@end

