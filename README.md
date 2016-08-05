# IOWait-Analysis

Instructions for use:

1. Clone repository using command: git clone https://github.com/corwynnielsen/IOWait-Analysis.git

2. Type python example_parser.py ______ with the underscores representing a command line argument
for a hostname directory containing tacc log files. Ex) 'python example_parser.py /home/USERNAME/taccstatsdata/Stampede/c403-104.stampede.tacc.utexas.edu'

3. Upon completion, the error data will be inserted into the 'ts_analysis' database using the user profile 'xdtas'.
Errors and reboots are also logged to a standard text file.
