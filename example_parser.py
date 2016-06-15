""" X """
import gzip
import logging
import string
import os
import sys
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

#pylint: disable = E1101
#Pylint does not properly recognize numpy arrays

def schema_fixup(type_name, desc):
    """ This function implements a workaround for a known issue with incorrect schema,
    definitions for irq, block and sched tacc_stats metrics. """

    if type_name == "irq":
        # All of the irq metrics are 32 bits wide
        res = ""
        for token in desc.split():
            res += token.strip() + ",W=32 "
        return res

    elif type_name == "sched":
        # Most sched counters are 32 bits wide with 3 exceptions
        res = ""
        sixtyfourbitcounters = ["running_time,E,U=ms", "waiting_time,E,U=ms", "pcount,E"]
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
            if token.startswith("syscall_") and (token.endswith("_s,E,U=s") or token.endswith("_ns,E,U=ns")):
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
    __slots__ = ('key', 'index', 'is_control', 'is_event', 'width', 'mult', 'unit')

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
                raise ValueError("unrecognized option `%s' in schema entry spec `%s'\n", opt, s)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               all(self.__getattribute__(attr) == other.__getattribute__(attr) \
                   for attr in self.__slots__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        lis = [] # 'index=%d' % self.index
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
        return '{' + ', '.join(("'%s': %s" % (k, repr(self[k]))) \
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


class SimpleTaccParser(object):
    """Takes Taccstats logs files and  """

    def __init__(self):

        self.procdump = None
        self.file_schemas = {}

        self.flag_reboot = 0

        self.dict_of_cpu_totals = {
            'cpu0': [], 'cpu1': [], 'cpu2': [], 'cpu3': [], 'cpu4': [],
            'cpu5': [], 'cpu6': [], 'cpu7': [], 'cpu8': [], 'cpu9': [],
            'cpu10': [], 'cpu11': [], 'cpu12': [], 'cpu13': [], 'cpu14': [],
            'cpu15': []
        }

        #self.error_dict holds the filename as well as a list of lists that
        #contain the cpu affected and timestamp
        self.error_dict = {}

        #self.dict_of_iowait_lists holds the values of each iowait value for
        #each cpu, clears after file ends
        self.dict_of_iowait_lists = {
            'cpu0': [], 'cpu1': [], 'cpu2': [], 'cpu3': [], 'cpu4': [],
            'cpu5': [], 'cpu6': [], 'cpu7': [], 'cpu8': [], 'cpu9': [],
            'cpu10': [], 'cpu11': [], 'cpu12': [], 'cpu13': [], 'cpu14': [],
            'cpu15': []
        }
        self.times = []
        self.raw_stats = {}
        self.marks = {}
        self.rotatetimes = []

        self.call_counter = 0
        self.state = ACTIVE
        self.timestamp = None
        self.filename = None
        self.fileline = None
        self.tacc_version = "Unknown"

        self.schemas = {}
        self.mismatch_schemas = {}


    def trace(self, fmt, *args):
        #pylint: disable = W1201
        #pylint incorrectly recognizing logging formatting
        logging.debug(fmt % args)

    def error(self, fmt, *args):
        #pylint: disable = W1201
        #pylint incorrectly recognizing logging formatting
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
                        # self.error("file `%s', type `%s', schema mismatch desc `%s'",
                        #            fp.name, type_name, schema_desc)
                elif char == SF_PROPERTY_CHAR:
                    if line.startswith("$tacc_stats"):
                        self.tacc_version = line.split(" ")[1].strip()
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
            self.error("file `%s' bad header on line %s", self.filename, self.fileline)
            return

        try:
            for line in filepath:
                self.fileline += 1
                self.parse(line.strip())
                if self.state == DONE:
                    break
        except Exception as any_exception:
            self.error("file `%s' exception %s on line %s", self.filename, str(any_exception), self.fileline)


    def parse(self, line):
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
        self.trace("TRANS {} -> {} ({})".format(STATENAMES[self.state], STATENAMES[newstate], reason))
        self.state = newstate

    def processtimestamp(self, line):
        """ process the timestamp """
        recs = line.strip().split(" ")
        try:
            self.timestamp = float(recs[0])
        except IndexError:
            self.error("syntax error timestamp in file '%s' line %s", self.filename, self.fileline)
            return

    def check_lists_for_discrepencies(self, any_dict, filename):

        """
        Checks the specified dictionary containing iowait numbers for
        inconsistencies, specificially drops in iowait values. Inconsistencies
        are added to a new dictionary with the file name as a key and the values
        being a list of lists containing the cpu number and timestamp
        """

        counter = 0
        if self.flag_reboot > 1:
            for dict_tuple in any_dict.items():
                iowait_nums = dict_tuple[1]
                for num in iowait_nums:
                    counter += 1
                    if counter < len(iowait_nums) and num > iowait_nums[counter]:
                        logging.error('Error with %s iowait numbers for %s, %s decreased to %s at %f', filename, dict_tuple[0], num, iowait_nums[counter], self.timestamp)
                        if filename not in self.error_dict:
                            self.error_dict[filename] = []
                        self.error_dict[filename].append([dict_tuple[0], self.timestamp])
                    if counter == len(iowait_nums):
                        counter = 0
        return self.error_dict

    def check_for_reboot(self, any_dict):

        """
        Checks if the previous total cpu total is greater than the current, if
        that is true, flag_reboot is set to true. The value at 0 is removed.
        """

        for dict_tuple in any_dict.items():
            cpu_sums = dict_tuple[1]
            if len(cpu_sums) == 2:
                if cpu_sums[0] > cpu_sums[1]:
                    logging.debug(dict_tuple[1])
                    self.flag_reboot += 1
                    logging.debug('Cpu counter %s', self.flag_reboot)
                cpu_sums.remove(cpu_sums[0])
            if self.flag_reboot is 16:
                reboot_info = 'Reboot at %s for %s' % (self.timestamp, self.filename)
                write_reboot_to_txt(reboot_info)
                self.flag_reboot = 1
                
    def processdata(self, line):

        """
        Takes in lines from tacc stats files and puts all of the data into a
        numpy array. Handles various schema related errors. This method also
        stores iowait values and cpu total timings into two different
        dictionaries while checking for reboots.
        """

        if self.state == ACTIVE or self.state == LAST_RECORD:

            try:
                type_name, dev_name, rest = line.split(None, 2)
            except ValueError:
                self.error("syntax error on file '%s' line %s", self.filename, self.fileline)
                return

            schema = self.file_schemas.get(type_name)
            if not schema:
                if not type_name in self.mismatch_schemas:
                    self.error("file `%s', unknown type `%s', discarding line `%s'",
                               self.filename, type_name, self.fileline)
                return

            vals = numpy.fromstring(rest, dtype=numpy.uint64, sep=' ')
            if vals.shape[0] != len(schema):
                self.error("file `%s', type `%s', expected %d values, read %d, discarding line `%s'", self.filename, type_name, len(schema), vals.shape[0], self.fileline)
                return

            #Checks the numpy array vals for necessary values and puts them in a
            #dictionary named dict_of_iowait_lists for further processing

            device_name = 'cpu%s' % (dev_name)
            get_schema = self.get_schema(type_name)
            if type_name == "cpu":
                cpu_timings = vals[[get_schema['nice'].index,
                get_schema['system'].index, get_schema['idle'].index,
                get_schema['iowait'].index, get_schema['irq'].index,
                get_schema['softirq'].index]]
                cpu_total = int(numpy.sum(cpu_timings))
                self.dict_of_cpu_totals[device_name].append(cpu_total)
                self.check_for_reboot(self.dict_of_cpu_totals)
                iowait_val = vals[self.get_schema(type_name)['iowait'].index]
                self.dict_of_iowait_lists[device_name].append(iowait_val)


    @property
    def get_dict_of_iowait_lists(self):

        """
        Getter for dict_of_iowait_lists, used in read_all_gz_files to provide
        a parameter for check_lists_for_discrepencies
        """

        return self.dict_of_iowait_lists

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
            self.error("syntax error processmark file `%s' line `%s'", self.filename, self.fileline)
            return
        if actions[0] == "end":

            self.trace("Seen end at %s for \"%s\"", self.timestamp, actions[1])

        if actions[0] == "begin":
            self.trace("Seen begin at %s for \"%s\"", self.timestamp, actions[1])

        if actions[0] == "rotate":
            if self.state == ACTIVE or self.state == ACTIVE_IGNORE:
                self.rotatetimes.append(self.timestamp)

        if actions[0] == "procdump":
            # procdump information is valid even when in active ignore
            if (self.state == ACTIVE or self.state == ACTIVE_IGNORE) and self.procdump != None:
                self.procdump.parse(line)

def extract_last_list_val(any_dict):

    """
    Takes the last iowait values from the current instance for each cpu and
    returns a list of those values in order to read all files as a
    contiguous block
    """

    list_of_last_vals = []
    for value in any_dict.values():
        list_of_last_vals.append(value[-1])
    return list_of_last_vals

def append_last_vals(a_list, any_dict):

    """
    Takes in a list and dictionary and inserts a value from a_list at an
    incrementing index into any_dict, one value for each key
    """

    indexer = 0
    for key in any_dict:
        any_dict[key].insert(0, a_list[indexer])
        indexer += 1

def write_reboot_to_txt(reboot_info):
    if not os.path.exists('dict_data.txt'):
        os.mknod('dict_data.txt')
    with open('reboot_data.txt', 'a') as rfile:
        rfile.write(reboot_info)
        rfile.write('\n')

def write_dict_to_txt(any_dict, filename):

    """
    Writes inputed dictionary into a text file. Used to write errors to a file.
    """

    if not os.path.exists('reboot_data.txt'):
        os.mknod('reboot_data.txt')
    with open('dict_data.txt', 'a') as afile:
        for filename, onelist in sorted(any_dict.items()):
            easier_read_format = '%s ---> %s' % (filename, onelist)
            afile.write(easier_read_format)
            afile.write('\n')

def read_all_gz_files(path):

    """
    Reads all '.gz' files in a given directory and checks subfolders. Checks all
    tacc stats files for errors and writes them to a file.
    """

    instance_count = 0
    filecount = 0
    list_of_gz_files = []

    #Collects all files in a directory into a list to sort

    list_of_gz_files = list_of_files_in_directory(path)

    if len(list_of_gz_files) != 0:
        for afile in sorted(list_of_gz_files):
            with gzip.open(afile) as filepath:
                filecount += 1
                stp = SimpleTaccParser()
                stp.read_stats_file(filepath)
                checker = stp.check_lists_for_discrepencies(stp.get_dict_of_iowait_lists, afile)
                write_dict_to_txt(checker, afile)
                extracter = extract_last_list_val(stp.get_dict_of_iowait_lists)

                #pylint: disable = E0601
                #Error says last_instance is accessed before assignment, but
                #cannot be accessed until instance_count>1 so it is irrelevent
                #necessary for append_last_vals to append properly

                if instance_count > 0:
                    append_last_vals(last_instance, stp.get_dict_of_iowait_lists)
                    last_instance = extracter
                instance_count = 1
                last_instance = extracter
        print 'Successfully read all %s files in directory' % (filecount)
    else:
        print 'No \'.gz\' files in %s' % (path)

def list_of_files_in_directory(source):

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
            print '%s: Oops %s doesn\'t appear to be a valid file path!' % (osexcept, sys.argv[1])

if __name__ == "__main__":
    main()
