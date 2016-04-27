
To obtain EEG data: 
  1. Open "EEGLogger" project with XCode
  2. Open "ViewController.mm" and make sure corresponding Emotiv device connect method calls are uncommented
  3. Start app, turn on device. Wait for device connected message on app
  4. XCode console log will start rolling once device data is coming in
  5. Log file located "/Users/_User_name_/Library/Developer/Xcode/DerivedData/EEGLogger/Build/Products/Debug/EEGDataLogger.csv"

Note: Emotiv Premium SDK will be required (The underlying native library provides raw EEG data access to device)
