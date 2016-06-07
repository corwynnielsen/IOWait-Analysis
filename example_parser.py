""" X """
import gzip
import logging
import numpy
import string
import os
import sys
import datetime
import threading

SF_SCHEMA_CHAR = '!'
SF_DEVICES_CHAR = '@'
SF_COMMENT_CHAR = '#'
SF_PROPERTY_CHAR = '$'
SF_MARK_CHAR = '%'

(PENDING_FIRST_RECORD, ACTIVE, ACTIVE_IGNORE, LAST_RECORD, DONE) = range(0, 5)
STATENAMES = {PENDING_FIRST_RECORD: "PENDING_FIRST_RECORD", ACTIVE: "ACTIVE", ACTIVE_IGNORE: "ACTIVE_IGNORE", LAST_RECORD: "LAST_RECORD", DONE: "DONE"}

def schema_fixup(type_name, desc):
    """ This function implements a workaround for a known issue with incorrect schema """
    """ definitions for irq, block and sched tacc_stats metrics. """

    if type_name == "irq":
        # All of the irq metrics are 32 bits wide
        res = ""
        for token in desc.split():
            res += token.strip() + ",W=32 "
        return res

    elif type_name == "sched":
        # Most sched counters are 32 bits wide with 3 exceptions
        res = ""
        sixtyfourbitcounters = [ "running_time,E,U=ms", "waiting_time,E,U=ms", "pcount,E" ]
        for token in desc.split():
            if token in sixtyfourbitcounters:
                res += token.strip() + " "
            else:
                res += token.strip() + ",W=32 "
        return res
    elif type_name == "block":
        # Most block counters are 64bits wide with a few exceptions
        res = ""
        thirtytwobitcounters = [ "rd_ticks,E,U=ms", "wr_ticks,E,U=ms", "in_flight", "io_ticks,E,U=ms", "time_in_queue,E,U=ms" ]
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
            if token.startswith("syscall_") and ( token.endswith("_s,E,U=s") or token.endswith("_ns,E,U=ns")):
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
        for i, s in enumerate(desc.split()):
            e = SchemaEntry(i, s)
            dict.__setitem__(self, e.key, e)
            self._key_list.append(e.key)
            self._value_list.append(e)

    def __iter__(self):
        return self._key_list.__iter__()

    def __repr__(self):
        return '{' + ', '.join(("'%s': %s" % (k, repr(self[k]))) \
                               for k in self._key_list) + '}'

    def _notsup(self, s):
        raise TypeError("'Schema' object does not support %s" % s)

    def __delitem__(self, k, v):
        self._notsup('item deletion')

    def pop(self, k, d=None):
        self._notsup('removal')

    def popitem(self):
        self._notsup('removal')

    def setdefault(self, k, d=None):
        self._notsup("item assignment")

    def update(self, **args):
        self._notsup("update")

    def items(self):
        return zip(self._key_list, self._value_list)

    def iteritems(self):fixup

    def iterkeys(self):
        return self._key_list.__iter__()

    def itervalues(self):
        return self._value_list.__iter__()

    def keys(self):
        return self._key_list

    def values(self):
        return self._value_list


class SimpleTaccParser(object):
    """ """

    def __init__(self):

        self.procdump = None

        #Lists to store IOWait data in dict_of_iowait_lists dictionary

        self.cpu0_list = []
        self.cpu1_list = []
        self.cpu2_list = []
        self.cpu3_list = []
        self.cpu4_list = []
        self.cpu5_list = []
        self.cpu6_list = []
        self.cpu7_list = []
        self.cpu8_list = []
        self.cpu9_list = []
        self.cpu10_list = []
        self.cpu11_list = []
        self.cpu12_list = []
        self.cpu13_list = []
        self.cpu14_list = []
        self.cpu15_list = []

        self.dict_of_iowait_lists = {}
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
        logging.debug(fmt % args)

    def error(self, fmt, *args):
        logging.error(fmt % args)

    def get_schema(self, type_name, desc=None):
        schema = self.schemas.get(type_name)
        if schema:
            if desc and schema.desc != schema_fixup(type_name,desc):
                # ...
                return None
        elif desc:
            desc = schema_fixup(type_name, desc)
            schema = self.schemas[type_name] = Schema(desc)
        return schema

    def read_stats_file_header(self, fp):
        file_schemas = {}
        for line in fp:
            self.fileline += 1
            try:
                c = line[0]
                if c == SF_SCHEMA_CHAR:
                    type_name, schema_desc = line[1:].split(None, 1)
                    schema = self.get_schema(type_name, schema_desc)
                    if schema:
                        file_schemas[type_name] = schema
                    else:
                        self.mismatch_schemas[type_name] = 1
                        self.error("file `%s', type `%s', schema mismatch desc `%s'",
                                   fp.name, type_name, schema_desc)
                elif c == SF_PROPERTY_CHAR:
                    if line.startswith("$tacc_stats"):
                        self.tacc_version = line.split(" ")[1].strip()
                    pass
                elif c == SF_COMMENT_CHAR:
                    pass
                else:
                    break
            except Exception as exc:
                self.error("file `%s', caught `%s' discarding line `%s'",
                           fp.name, exc, line)
                break
        return file_schemas


    def read_stats_file(self, fp):

        if self.state == DONE:
            return

        self.filename = fp.name
        self.fileline = 0

        self.file_schemas = self.read_stats_file_header(fp)

        if not self.file_schemas:
            self.error("file `%s' bad header on line %s", self.filename, self.fileline)
            return

        try:
            for line in fp:
                self.fileline += 1
                self.parse(line.strip())
                if self.state == DONE:
                    break
        except Exception as e:
            self.error("file `%s' exception %s on line %s", self.filename, str(e), self.fileline)


    def parse(self, line):
        if len(line) < 1:
            return

        ch = line[0]

        if ch.isdigit():
            self.processtimestamp(line)
        elif ch.isalpha():
            self.processdata(line)
        elif ch == SF_SCHEMA_CHAR:
            self.processschema(line)
            try:
                self.processmark(line)
            except TypeError as e:
                self.error("TypeError %s in %s line %s", str(e), self.filename, self.fileline)
        else:
            logging.warning("Unregognised character \"%s\" in %s on line %s ",
                            ch, self.filename, self.fileline)
            pass

    def setstate(self, newstate, reason = None):
        self.trace("TRANS {} -> {} ({})".format( STATENAMES[self.state], STATENAMES[newstate], reason ) )
        self.state = newstate

    def processtimestamp(self, line):
        """ process the timestamp """
        recs = line.strip().split(" ")
        try:
            self.timestamp = float(recs[0])
            jobs = recs[1].strip().split(",")
        except IndexError as e:
            self.error("syntax error timestamp in file '%s' line %s", self.filename, self.fileline)
            return

    def processdata(self,line):

        if self.state == ACTIVE or self.state == LAST_RECORD:

            try:
                type_name, dev_name, rest = line.split(None, 2)
            except ValueError as e:
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
                self.error("file `%s', type `%s', expected %d values, read %d, discarding line `%s'",
                       self.filename, type_name, len(schema), vals.shape[0], self.fileline)
                return

            type_stats = self.raw_stats.setdefault(type_name, {})
            dev_stats = type_stats.setdefault(dev_name, [])
            #if type_name == "cpu":
                #logging.debug("Io wait counter for %s %s is %s", type_name, dev_name, vals[self.get_schema(type_name)['iowait'].index])

            #Dictionary with the key being the cpu core number and the value being a list of IOWait times for each cpu
            self.dict_of_iowait_lists = {'cpu0': self.cpu0_list, 'cpu1': self.cpu1_list, 'cpu2': self.cpu2_list, 'cpu3': self.cpu3_list, 'cpu4': self.cpu4_list,
            'cpu5': self.cpu5_list, 'cpu6': self.cpu6_list, 'cpu7': self.cpu7_list, 'cpu8': self.cpu8_list, 'cpu9': self.cpu9_list, 'cpu10': self.cpu10_list,
            'cpu11': self.cpu11_list, 'cpu12': self.cpu12_list, 'cpu13': self.cpu13_list, 'cpu14': self.cpu14_list, 'cpu15': self.cpu15_list,}
            matching_key_check = 'cpu%s' % (dev_name)

            if type_name == "cpu":
                iowait_val = vals[self.get_schema(type_name)['iowait'].index]
                for key in self.dict_of_iowait_lists.keys():
                    if key == matching_key_check:
                        self.dict_of_iowait_lists[key].append(iowait_val)

            self.write_dict_to_txt(self.dict_of_iowait_lists)
            check_lists_for_discrepencies(self.dict_of_iowait_lists)

    def write_dict_to_txt(self, any_dict):
        if not os.path.exists('dict_data.txt'):
            os.mknod("dict_data.txt")
            'New file \'dict_data\' created'
        with open('dict_data.txt', 'r+') as f:
                f.write(str(any_dict))


    def processschema(self,line):
        print "processschema"
        pass

    def processproperty(self,line):
        print "processproperty"
        pass


    def processmark(self,line):
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
        pass


def read_all_gz_files(path):
    filecount = 0
    list_of_gz_files = []
    STP = SimpleTaccParser()
    for gz_file in os.listdir(path):
        if gz_file.endswith('.gz'):
            list_of_gz_files.append(gz_file)
            for afile in list_of_gz_files:
                list_of_gz_files = sorted(list_of_gz_files)
                fullpath = os.path.join(path, afile)
                with gzip.open(fullpath) as fp:
                    filecount += 1
                    print 'Opening file...%s' % (fullpath)
                    iowait_debug_text = STP.read_stats_file(fp)
    print 'Successfully read all files in directory'

def check_lists_for_discrepencies(any_dict):
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%S',
                        level=logging.DEBUG)
    counter = 0
    #test_dict = {'a' : [123, 123, 126], 'b' : [123, 122, 126], 'c' : [123, 122, 126]}
    #any_dict = test_dict
    for tuple in any_dict.items():
        iowait_nums = tuple [1]
        for num in iowait_nums:
            iowait_nums_index = iowait_nums.index(num)
            counter += 1
            if counter < len(iowait_nums) and num > iowait_nums[iowait_nums.index(num)+1]:
                logging.DEBUG('Error with iowait numbers for list %s at indices %s and %s which had %s decrease to %s' % (tuple[0] ,iowait_nums_index, iowait_nums_index+1, iowait_nums[iowait_nums_index],iowait_nums[iowait_nums_index+1]))
            if counter == len(iowait_nums):
                counter = 0

def main():

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%S',
                        level=logging.DEBUG)

    path = '/home/corwynni/taccstats_data/stampede/test'
    read_all_gz_files(path)
    #check_lists_for_discrepencies()


if __name__ == "__main__":
    main()
