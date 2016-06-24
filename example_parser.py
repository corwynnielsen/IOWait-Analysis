""" X """
import gzip
import logging
import string
import os
import sys
import time
import datetime
import re
import numpy

SF_SCHEMA_CHAR = '!'
SF_DEVICES_CHAR = '@'
SF_COMMENT_CHAR = '#'
SF_PROPERTY_CHAR = '$'
SF_MARK_CHAR = '%'

(PENDING_FIRST_RECORD, ACTIVE, ACTIVE_IGNORE, LAST_RECORD, DONE) = range(0, 5)
STATENAMES = {
    PENDING_FIRST_RECORD: "PENDING_FIRST_RECORD",
    ACTIVE: "ACTIVE",
    ACTIVE_IGNORE: "ACTIVE_IGNORE",
    LAST_RECORD: "LAST_RECORD",
    DONE: "DONE"
}

# pylint: disable = E1101
# Pylint does not properly recognize numpy arrays


def schema_fixup(type_name, desc):

    """
    This function implements a workaround for a known issue with incorrect
    schema, definitions for irq, block and sched tacc_stats metrics.
    """

    if type_name == "irq":
        # All of the irq metrics are 32 bits wide
        res = ""
        for token in desc.split():
            res += token.strip() + ",W=32 "
        return res

    elif type_name == "sched":
        # Most sched counters are 32 bits wide with 3 exceptions
        res = ""
        sixtyfourbitcounters = ["running_time,E,U=ms",
                                "waiting_time,E,U=ms",
                                "pcount,E"]
        for token in desc.split():
            if token in sixtyfourbitcounters:
                res += token.strip() + " "
            else:
                res += token.strip() + ",W=32 "
        return res
    elif type_name == "block":
        # Most block counters are 64bits wide with a few exceptions
        res = ""
        thirtytwobitcounters = [
            "rd_ticks,E,U=ms",
            "wr_ticks,E,U=ms",
            "in_flight", "io_ticks,E,U=ms",
            "time_in_queue,E,U=ms"
            ]
        for token in desc.split():
            if token in thirtytwobitcounters:
                res += token.strip() + ",W=32 "
            else:
                res += token.strip() + " "
        return res
    elif type_name == "panfs":
        # The syscall_*_(n+)s stats are not events
        res = ""
        for token in desc.split():
            token = token.strip()
            if token.startswith("syscall_") and (token.endswith("_s,E,U=s") or
                                                 token.endswith("_ns,E,U=ns")):
                res += string.replace(token, "E,", "") + " "
            else:
                res += token + " "
        return res
    elif type_name == "ib":
        res = ""
        for token in desc.split():
            token = token.strip()
            if not token.endswith(",W=32"):
                res += token.strip() + ",W=32 "
            else:
                res += token.strip() + " "
        return res

    return desc


class SchemaEntry(object):
    __slots__ = ('key',
                 'index',
                 'is_control',
                 'is_event',
                 'width',
                 'mult',
                 'unit')

    def __init__(self, i, s):
        opt_lis = s.split(',')
        self.key = opt_lis[0]
        self.index = i
        self.is_control = False
        self.is_event = False
        self.width = None
        self.mult = None
        self.unit = None
        for opt in opt_lis[1:]:
            if len(opt) == 0:
                continue
            elif opt[0] == 'C':
                self.is_control = True
            elif opt[0] == 'E':
                self.is_event = True
            elif opt[0:2] == 'W=':
                self.width = int(opt[2:])
            elif opt[0:2] == 'U=':
                j = 2
                while j < len(opt) and opt[j].isdigit():
                    j += 1
                if j > 2:
                    self.mult = numpy.uint64(opt[2:j])
                if j < len(opt):
                    self.unit = opt[j:]
                if self.unit == "KB":
                    self.mult = numpy.uint64(1024)
                    self.unit = "B"
            else:
                # XXX
                raise ValueError("unrecognized option `%s' in schema entry spec \
                                 `%s'\n", opt, s)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               all(self.__getattribute__(attr) == other.__getattribute__(attr)
                   for attr in self.__slots__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        lis = []  # 'index=%d' % self.index
        if self.is_event:
            lis.append('is_event=True')
        elif self.is_control:
            lis.append('is_control=True')
        if self.width:
            lis.append('width=%d' % int(self.width))
        if self.mult:
            lis.append('mult=%d' % int(self.mult))
        if self.unit:
            lis.append('unit=%s' % self.unit)
        return '(' + ', '.join(lis) + ')'


class Schema(dict):
    def __init__(self, desc):
        dict.__init__(self)
        self.desc = desc
        self._key_list = []
        self._value_list = []
        for index, schema in enumerate(desc.split()):
            entry_point = SchemaEntry(index, schema)
            dict.__setitem__(self, entry_point.key, entry_point)
            self._key_list.append(entry_point.key)
            self._value_list.append(entry_point)

    def __iter__(self):
        return self._key_list.__iter__()

    def __repr__(self):
        return '{' + ', '.join(("'%s': %s" % (k, repr(self[k])))
                               for k in self._key_list) + '}'

    def _notsup(self, unsupported):
        raise TypeError("'Schema' object does not support %s" % unsupported)

    def __delitem__(self, k):
        self._notsup('item deletion')

    def pop(self):
        self._notsup('removal')

    def popitem(self):
        self._notsup('removal')

    def setdefault(self):
        self._notsup("item assignment")

    def update(self):
        self._notsup("update")

    def items(self):
        return zip(self._key_list, self._value_list)

    def iteritems(self):
        for k in self._key_list:
            yield (k, dict.__getitem__(self, k))

    def iterkeys(self):
        return self._key_list.__iter__()

    def itervalues(self):
        return self._value_list.__iter__()

    def keys(self):
        return self._key_list

    def values(self):
        return self._value_list


class RebootData(object):

    """
    Used to store data in between instances of the SimpleTaccParser,
    specifically for detecting a reboot between files. The list
    self.last_cpu_total_vals, which are the last sums of the cpu timings, is
    stored here. Unless the file is the first in the directory, which is
    checked using self.not_first_file, those values are then appended into the
    new self.dict_of_cpu_total_timings that is empty upon instantiation of
    SimpleTaccParser. Also handles storing the reboot data text file's file
    name and ensuring only one file is made via the boolean self.file_created
    """

    def __init__(self):
        self.last_cpu_total_vals = []
        self.not_first_file = False
        self.file_created = False
        self.reboot_data_filename = None

    def set_last_cpu_total_vals(self, a_list):

        """
        Mutates the instance variable self.last_cpu_total_vals, used to store
        total cpu timing data from a previous instance of SimpleTaccParser
        """

        self.last_cpu_total_vals = a_list

    def set_not_first_file(self, new_bool):

        """
        Mutates the instance variable self.set_reboot_between_files_bool, used
        to signify that the file being read is no longer the first file
        """

        self.not_first_file = new_bool

    def set_reboot_data_filename(self, name_string):

        """
        Mutates the instance variable self.set_reboot_data_filename, used to
        store the filename between instances so multiple different files are
        not created
        """

        self.reboot_data_filename = name_string

    def set_file_created(self, new_bool):

        """
        Mutates the instance variable self.file_created, used to check if a
        file have been created
        """

        self.file_created = new_bool

STORE_REBOOT_DATA = RebootData()
# global variable so the values stored can be
# acessed throughout the whole reading files process
# Possibly should be changed to be a class var?
# Martins says should be ok


class SimpleTaccParser(object):

    """
    Takes tacc stats log files and parses them using read_stats_file_header,
    read_stats_file, and parse. After parsing, the iowait numbers are inspected
    for drops. If a drop is found, the drop is documented to a file with a
    timestamp. Reboots with not be falsely represented as errors and are
    instead logged to their own file with a timestamp. Reboots are found by
    adding all cpu timings together and comparing the current sum to the
    previous and checking for drops between one timestamp to the next for each
    device. Checks for reboots and iowait drops in between files.
    """

    def __init__(self):

        self.procdump = None
        self.file_schemas = {}

        self.file_created = False
        self.reboot_flag = False # used to prevent check_lists_for_discrepencies from incorrectly reporting errors
        self.device_potential_reboot_counter = 0

        self.dict_of_cpu_total_timings = {}  # holds the total timings on each device, used to find reboots

        self.error_dict = {}      # self.error_dict holds the filename as well as a list of lists that
                                  # contain the cpu affected and timestamp

        self.dict_of_iowait_lists = {}

        self.last_cpu_totals = []

        self.list_of_timestamps = [] #stores timestamp for use in check_lists_for_discrepencies

        self.counter = 0
        self.raw_stats = {}
        self.marks = {}
        self.rotatetimes = []

        self.call_counter = 0
        self.state = ACTIVE
        self.hostname = None
        self.timestamp = None
        self.filename = None
        self.fileline = None
        self.tacc_version = "Unknown"

        self.schemas = {}
        self.mismatch_schemas = {}

    def trace(self, fmt, *args):
        # pylint: disable = W1201
        # pylint incorrectly recognizing logging formatting error
        logging.debug(fmt % args)

    def error(self, fmt, *args):
        # pylint: disable = W1201
        # pylint incorrectly recognizing logging formatting error
        logging.error(fmt % args)

    def get_schema(self, type_name, desc=None):
        schema = self.schemas.get(type_name)
        if schema:
            if desc and schema.desc != schema_fixup(type_name, desc):
                # ...
                return None
        elif desc:
            desc = schema_fixup(type_name, desc)
            schema = self.schemas[type_name] = Schema(desc)
        return schema

    def read_stats_file_header(self, filepath):
        file_schemas = {}
        for line in filepath:
            self.fileline += 1
            try:
                char = line[0]
                if char == SF_SCHEMA_CHAR:
                    type_name, schema_desc = line[1:].split(None, 1)
                    schema = self.get_schema(type_name, schema_desc)
                    if schema:
                        file_schemas[type_name] = schema
                    else:
                        self.mismatch_schemas[type_name] = 1
                        # self.error("file `%s', type `%s', schema mismatch desc \
                        # `%s'", filepath.name, type_name, schema_desc)
                elif char == SF_PROPERTY_CHAR:
                    if line.startswith("$tacc_stats"):
                        self.tacc_version = line.split(" ")[1].strip()
                    if line.startswith("$hostname"):
                        self.hostname = line.split(" ")[1].strip()
                elif char == SF_COMMENT_CHAR:
                    pass
                else:
                    break
            except Exception as exc:
                self.error("file `%s', caught `%s' discarding line `%s'",
                           filepath.name, exc, line)
                break
        return file_schemas

    def read_stats_file(self, filepath):

        if self.state == DONE:
            return

        self.filename = filepath.name
        self.fileline = 0

        self.file_schemas = self.read_stats_file_header(filepath)

        if not self.file_schemas:
            self.error("file `%s' bad header on line %s",
                       self.filename, self.fileline)
            return

        try:
            for line in filepath:
                self.fileline += 1
                self.parse(line.strip())
                if self.state == DONE:
                    break
        except Exception as any_exception:
            self.error("file `%s' exception %s on line %s",
                       self.filename, str(any_exception), self.fileline)

    def parse(self, line):

        """
        Reads the tacc stats data file line by line, dealing with schema
        character as they come. If the character is unrecognized, an error is
        thrown
        """

        if len(line) < 1:
            return

        char = line[0]

        if char.isdigit():
            self.processtimestamp(line)
        elif char.isalpha():
            self.processdata(line)
        elif char == SF_SCHEMA_CHAR:
            self.processschema()
        elif char == SF_PROPERTY_CHAR:
            pass
        elif char == SF_MARK_CHAR:
            pass
        else:
            logging.warning("Unregognised character \"%s\" in %s on line %s ",
                            char, self.filename, self.fileline)

    def setstate(self, newstate, reason=None):

        """
        Alters the state machine in the class
        """

        self.trace("TRANS {} -> {} ({})".format(STATENAMES[self.state],
                                                STATENAMES[newstate], reason))
        self.state = newstate

    def processtimestamp(self, line):

        """
        process the timestamp
        """

        recs = line.strip().split(" ")
        try:
            self.timestamp = float(recs[0])
        except IndexError:
            self.error("syntax error timestamp in file '%s' line %s",
                       self.filename, self.fileline)
            return

    def check_lists_for_discrepencies(self, any_dict, filename):

        """
        Checks the specified dictionary containing iowait numbers for drops in
        iowait values. Unless a reboot is detected, inconsistencies are added
        to a new dictionary with the file name as a key and the values being a
        list of lists containing the cpu number and timestamp.
        """

        self.extract_last_cpu_totals(self.dict_of_cpu_total_timings)

        counter = 0
        for dict_tuple in any_dict.items():
            iowait_nums = dict_tuple[1]
            for num in iowait_nums:
                counter += 1
                if counter < len(iowait_nums) and num > iowait_nums[counter] \
                   and num is not 'flagged':
                    logging.error('Error with %s iowait numbers for %s, %s \
decreased to %s at %f', filename, dict_tuple[0], num, iowait_nums[counter],
                                  self.list_of_timestamps[counter-1])
                    if filename not in self.error_dict:
                        self.error_dict[filename] = []
                    timestamp = self.list_of_timestamps[counter-1]
                    self.error_dict[filename].append([dict_tuple[0],
                                                      timestamp])
                if counter == len(iowait_nums):
                    counter = 0
        return self.error_dict

    def check_for_reboot(self, any_dict):

        """
        Checks if the previous total cpu total is greater than the current, if
        that is true, flag_reboot is set to true. The value at 0 in cpu_sums is
        removed. If all devices are found to have rebooted, a timestamp and
        filename is written to a file for logging purposes.
        """

        for dict_tuple in any_dict.items():
            cpu_sums = dict_tuple[1]
            if len(cpu_sums) == 2:
                if cpu_sums[0] > cpu_sums[1]:
                    self.device_potential_reboot_counter += 1
                    self.reboot_flag = True
                cpu_sums.remove(cpu_sums[0])
            if self.device_potential_reboot_counter is 16:
                if not STORE_REBOOT_DATA.file_created:
                    STORE_REBOOT_DATA.set_reboot_data_filename(generate_timestamped_txt('reboot_data'))
                    STORE_REBOOT_DATA.set_file_created(True)
                reboot_info = 'Reboot at %f for %s' % (self.timestamp,
                                                       self.filename)
                logging.debug(reboot_info)
                write_reboot_data_to_txt(reboot_info, STORE_REBOOT_DATA.reboot_data_filename)
                self.device_potential_reboot_counter = 0

    def extract_last_cpu_totals(self, cpu_timings_dict):

        """
        Takes slices from the inputed numpy array, the amount of slices
        taken depends on the length of dict_of_iowait_lists which represents
        device count
        """

        self.last_cpu_totals = []
        for key in cpu_timings_dict:
            self.last_cpu_totals.insert(0, cpu_timings_dict[key][0])

    def processdata(self, line):

        """
        Takes in lines from tacc stats files and puts all of the data into a
        numpy array. Handles various schema related errors. This method also
        stores iowait values and cpu total timings into two different
        dictionaries while checking for reboots and flagging reboots when
        a condition is fulfilled.
        """

        if self.state == ACTIVE or self.state == LAST_RECORD:

            try:
                type_name, dev_name, rest = line.split(None, 2)
            except ValueError:
                self.error("syntax error on file '%s' line %s",
                           self.filename, self.fileline)
                return

            schema = self.file_schemas.get(type_name)
            if not schema:
                if type_name not in self.mismatch_schemas:
                    self.error("file `%s', unknown type `%s', \
                               discarding line `%s'",
                               self.filename, type_name, self.fileline)
                return

            vals = numpy.fromstring(rest, dtype=numpy.uint64, sep=' ')
            if vals.shape[0] != len(schema):
                self.error("file `%s', type `%s', expected %d values, read %d, \
                           discarding line `%s'",
                           self.filename, type_name, len(schema),
                           vals.shape[0], self.fileline)
                return

            device_name = 'cpu%s' % (dev_name)
            get_schema = self.get_schema(type_name)

            if type_name == "cpu":
                if self.timestamp not in self.list_of_timestamps:
                    self.list_of_timestamps.append(self.timestamp)

                if device_name not in self.dict_of_iowait_lists: #populates dictionaries with the necessary amount of keys
                    self.dict_of_iowait_lists[device_name] = []
                    self.dict_of_cpu_total_timings[device_name] = []

                cpu_timings = vals[[get_schema['nice'].index,
                                    get_schema['system'].index,
                                    get_schema['idle'].index,
                                    get_schema['iowait'].index,
                                    get_schema['irq'].index,
                                    get_schema['softirq'].index]]

                cpu_total = int(numpy.sum(cpu_timings))

                if STORE_REBOOT_DATA.not_first_file and self.counter < 16:
                    self.dict_of_cpu_total_timings[device_name].append(STORE_REBOOT_DATA.last_cpu_total_vals[0])
                    del STORE_REBOOT_DATA.last_cpu_total_vals[0]
                    self.counter += 1

                self.dict_of_cpu_total_timings[device_name].append(cpu_total)
                self.check_for_reboot(self.dict_of_cpu_total_timings)
                iowait_val = vals[self.get_schema(type_name)['iowait'].index]

                if self.reboot_flag:
                    self.dict_of_iowait_lists[device_name].append('flagged')
                    self.reboot_flag = False

                else:
                    self.dict_of_iowait_lists[device_name].append(iowait_val)

    @property
    def get_dict_of_iowait_lists(self):

        """
        Getter for dict_of_iowait_lists, used in read_all_gz_files to provide
        a parameter for check_lists_for_discrepencies
        """

        return self.dict_of_iowait_lists

    @property
    def get_dict_of_cpu_total_timings(self):

        """
        Getter for dict_of_cpu_total_timings, used to append last_cpu_totals
        into dict_of_cpu_total_timings to detect reboots across files, not sure
        if necessary yet as the appending is not working
        """

        return self.dict_of_cpu_total_timings

    @property
    def get_last_cpu_totals(self):

        """
        Getter for last_cpu_totals, each value is appended into
        dict_of_cpu_total_timings to detect reboots across files, not sure if
        necessary yet as the appending is not working
        """

        return self.last_cpu_totals

    @staticmethod
    def processschema():
        print "processschema"

    @staticmethod
    def processproperty():
        print "processproperty"

    def processmark(self, line):
        mark = line[1:].strip()
        actions = mark.split()
        if not actions:
            self.error("syntax error processmark file `%s' line `%s'",
                       self.filename, self.fileline)
            return
        if actions[0] == "end":

            self.trace("Seen end at %s for \"%s\"", self.timestamp, actions[1])

        if actions[0] == "begin":
            self.trace("Seen begin at %s for \"%s\"",
                       self.timestamp, actions[1])

        if actions[0] == "rotate":
            if self.state == ACTIVE or self.state == ACTIVE_IGNORE:
                self.rotatetimes.append(self.timestamp)

        if actions[0] == "procdump":
            # procdump information is valid even when in active ignore
            if (self.state == ACTIVE or self.state == ACTIVE_IGNORE) and \
               self.procdump is not None:
                self.procdump.parse(line)


def extract_last_list_val(any_dict):

    """
    Used to read gz files as a contiguous block. Takes the last iowait values
    from the current instance for each cpu and returns a list of those values
    in order to read all files as a contiguous block
    """

    list_of_last_vals = []
    for value in any_dict.values():
        list_of_last_vals.append(value[-1])
    return list_of_last_vals


def append_last_vals(a_list, any_dict):

    """
    Used to read gz files as a contiguous block. Takes in a list and dictionary
    and inserts a value from a_list at an incrementing index into any_dict, one
    value for each key
    """

    incrementer = 0
    for key in any_dict:
        any_dict[key].insert(0, a_list[incrementer])
        incrementer += 1


def generate_timestamped_txt(text_type):

    """
    Creates a text file which has a timestamp in the name
    """

    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    full_filename = '%s_%s.txt' % (text_type, timestamp)
    os.mknod(full_filename)
    return full_filename


def write_reboot_data_to_txt(reboot_info, filename):

    """
    Takes in the string from check_for_reboot and appends it into a file named
    'reboot_data', logs reboot data.
    """

    with open(filename, 'a') as rfile:
        rfile.write(reboot_info)
        rfile.write('\n')


def write_dict_to_txt(any_dict, dict_text_filename, hostname_dict):

    """
    Writes inputed dictionary into a text file. Used to write errors to a file.
    """

    with open(dict_text_filename, 'a') as afile:
        for filename, onelist in any_dict.items():
            regex = re.search(r"(\w+-\w+.stampede.tacc.utexas.edu)", filename).group()
            hostname_dict[regex].append(onelist)

        for hostname, error in hostname_dict.items():
            easier_read_format = '%s ---> %s' % (hostname, error)
            if len(hostname_dict[hostname]) is not 0:
                afile.write(easier_read_format)
                afile.write('\n')

def hostname_re(file_list):
    hostnames = {}
    for filename in file_list:
        regex = re.search(r"(\w+-\w+.stampede.tacc.utexas.edu)", filename)
        if regex.group() not in hostnames:
            hostnames[regex.group()] = []
    return hostnames

def read_all_gz_files(path):

    """
    Reads all '.gz' files in a given directory and checks subfolders. Checks
    all tacc stats files for errors and writes them to a file.
    """

    filecount = 0
    txt_filename = generate_timestamped_txt('dict_text')
    list_of_gz_files = []
    start_time = time.time()
    iowait_last_instance = None
    iowait_extracter = None
    # Collects all files in a directory into a list to sort

    list_of_gz_files = get_list_of_files_in_directory(path)
    hostnames = hostname_re(list_of_gz_files)

    if len(list_of_gz_files) != 0:  # If there is gz files in directory or items children
        for afile in sorted(list_of_gz_files):
            with gzip.open(afile) as filepath:
                filecount += 1
                stp = SimpleTaccParser()
                stp.read_stats_file(filepath)
                if iowait_last_instance is not None:  # used ensure the script is not in the first instance of stp
                    append_last_vals(iowait_last_instance,
                                     stp.get_dict_of_iowait_lists)
                    iowait_last_instance = iowait_extracter
                checker = stp.check_lists_for_discrepencies(
                    stp.get_dict_of_iowait_lists, afile)
                write_dict_to_txt(checker, txt_filename, hostnames)
                iowait_extracter = extract_last_list_val(
                    stp.get_dict_of_iowait_lists)
                iowait_last_instance = iowait_extracter
                STORE_REBOOT_DATA.set_not_first_file(True) # boolean set to signify the first file is done
                STORE_REBOOT_DATA.set_last_cpu_total_vals(stp.last_cpu_totals)  # sets the list in the RebootData class in order to maintain
                                                                                # cpu total timings across files

        print 'Read all %s files in directory in %d seconds' % (
            filecount, time.time() - start_time)
    else: # If there are no gz files in directory or its children
        print 'No \'.gz\' files in %s' % (path)


def get_list_of_files_in_directory(source):

    """
    Walks the file structure and finds all files ending with .gz which are then
    added to a list named match, match is returned
    """

    match = []
    for root, dirnames, filenames in os.walk(source):
        for filename in filenames:
            if filename.endswith('.gz'):
                match.append(os.path.join(root, filename))
    return match


def main():

    """
    Main method takes in as many file arguments as necessary and checks each
    directory's files for errors. Handles if file path is invalid.
    """

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%S',
                        level=logging.DEBUG)
    file_names = sys.argv[1:]
    if len(file_names) == 0:
        print 'Please input a directory that holds \'.gz\' files'
    else:
        try:
            print 'Reading files from directory: %s' % (sys.argv[1])
            read_all_gz_files(sys.argv[1])
        except OSError as osexcept:
            print '%s: Oops %s doesn\'t appear to be a valid file path!' % (
                osexcept, sys.argv[1])

if __name__ == "__main__":
    main()
