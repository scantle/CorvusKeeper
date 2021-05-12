import datetime as dt
import numpy as np
import pandas as pd
from os import path


# ----------------------------------------------------------------------------------------------------------------------#

class RavenFileReader(object):

    def __init__(self, filename):
        self.filename = filename
        self.path = path.dirname(filename)
        self.fileobject = None
        self._backburner = []

    def __enter__(self):
        self.fileobject = open(self.filename, 'r')
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.fileobject.close()

    def eof_check(self):
        """
        True if end of file (EOF) has been reached, false if otherwise.
        Reads chunk and if it gets nothing returned EOF is assumed
        """
        eof = False
        curr_pos = self.fileobject.tell()
        # print(curr_pos, self.st_size)
        chunk = self.fileobject.read(25)
        if chunk == '':
            # Is there something on the back burner??
            if len(self._backburner) > 0:
                self.fileobject = self._backburner.pop()
            else:
                eof = True
        else:
            self.fileobject.seek(curr_pos)
        return eof

    def nextline(self):
        return self.fileobject.readline().strip()

    def nexttag(self):
        line = False
        while self.eof_check() == False:
            line = self.nextline()
            # Redirect
            if line.startswith(':RedirectToFile'):
                nextfile = path.join(self.path, line.replace(':RedirectToFile', '').strip())
                if path.exists(nextfile):
                    self._backburner.append(self.fileobject)
                    self.fileobject = open(nextfile, 'r')
                else:
                    raise RuntimeError('RedirectToFile path invalid: {} \nIn File:'.format(nextfile, self.filename))
            elif line.startswith(':'):
                break
        else:
            line = False
        return line

    def read_dateline(self):
        line = self.nextline().split()
        start = dt.datetime.strptime('{} {}'.format(line[0], line[1]), '%Y-%m-%d  %H:%M:%S')
        delta = dt.timedelta(days=float(line[2]))
        nvalues = int(line[3])
        return (start, delta), nvalues

    def skiplines(self, lines):
        """Skips 'lines' number of lines in current file
        """
        for i in range(0, lines):
            self.fileobject.readline()

    def comma_detector(self):
        curr_pos = self.fileobject.tell()
        line = self.nextline()
        comma = False
        # A bold presumption, perhaps
        if ',' in line:
            comma = True
        self.fileobject.seek(curr_pos)
        return comma

    def read_RavenFrame(self, nvalues: int = None, header=True, names=None,
                        id_col=False, time_tuple=None, na_values='-1.2345'):
        units = []
        parameters = names
        if header:
            if parameters is None:
                parameters = self.nexttag().replace(',', ' ').split()[1:]
            units = self.nexttag().replace(',', ' ').split()[1:]
            if id_col:
                parameters.insert(0, 'ID')
        # Make sure pandas doesn't try to infer a header
        header = None
        curr_pos = self.fileobject.tell()  # Pandas frequently loses it's place
        if nvalues is None:
            nvalues = self.get_datadist()
        if self.comma_detector():
            df = pd.read_table(filepath_or_buffer=self.fileobject,
                               sep=',',
                               header=header,
                               names=parameters,
                               nrows=nvalues,
                               na_values=na_values)
        else:
            df = pd.read_table(filepath_or_buffer=self.fileobject,
                               delim_whitespace=True,
                               header=header,
                               names=parameters,
                               nrows=nvalues,
                               na_values=na_values)
        # Correct place in file
        self.fileobject.seek(curr_pos)
        self.skiplines(nvalues)
        # Adjust index if time is passed
        if time_tuple:
            # Start + time delta for nvalues
            dates = [time_tuple[0] + time_tuple[1] * d for d in range(0, nvalues)]
            df['Datetime'] = dates
            df = df.set_index('Datetime')
        return df, units

    def get_datadist(self):
        """ Finds the distance to the next tag (:) blank line
        """
        counter = 0
        last_pos = self.fileobject.tell()
        for line in self.fileobject:
            if line.strip().startswith(':') or line == '':
                break
            counter += 1
        self.fileobject.seek(last_pos)
        return counter


# ----------------------------------------------------------------------------------------------------------------------#

class RavenFile(object):

    @staticmethod
    def cleantag(line):
        cleaned = line.strip().replace(':', '')
        if len(cleaned.split()) > 1:
            cleaned = cleaned.split()[0]
        return cleaned


# ----------------------------------------------------------------------------------------------------------------------#

class RavenRVT(RavenFile):

    def __init__(self, filename=None):
        self.metgauges = {}
        self.obsgauges = {}
        if filename:
            self.read(filename)

    def read(self, filename):
        with RavenFileReader(filename) as f:
            line = f.nexttag()
            while line:
                # Begin data type checks
                if self.cleantag(line) == 'Gauge':
                    self.read_metgauge(line, f)
                elif self.cleantag(line) == 'ObservationData':
                    self.read_obsgauge(line, f)
                # Next line
                line = f.nexttag()

    def read_metgauge(self, line: str, f: RavenFileReader):
        name = line.split()[1]
        gauge_dict = {}
        line = f.nexttag()
        while line:
            # Begin data type checks
            if self.cleantag(line) == 'EndGauge':
                break
            elif self.cleantag(line) == 'Latitude':
                gauge_dict['Latitude'] = float(line.split()[1])
            elif self.cleantag(line) == 'Longitude':
                gauge_dict['Longitude'] = float(line.split()[1])
            elif self.cleantag(line) == 'Elevation':
                gauge_dict['Elevation'] = float(line.split()[1])
            elif self.cleantag(line) == 'MultiData':
                time_tuple, nvalues = f.read_dateline()
                data, units = f.read_RavenFrame(nvalues=nvalues, time_tuple=time_tuple)
                gauge_dict['Type'] = 'MultiData'
                gauge_dict['Data'] = data
                gauge_dict['Units'] = units
                self.metgauges[name] = gauge_dict
            elif self.cleantag(line) == 'Data':
                # TODO test
                gauge_dict['Type'] = line.split()[1]
                gauge_dict['Units'] = line.split()[2]
                time_tuple, nvalues = f.read_dateline()
                data, units = f.read_RavenFrame(nvalues=nvalues, header=False, time_tuple=time_tuple)
                gauge_dict['Data'] = data
                self.metgauges[name] = gauge_dict
            # Next line
            line = f.nexttag()

    def read_obsgauge(self, line: str, f: RavenFileReader):
        gauge_dict = {}
        gauge_dict['Type'] = line.split()[1]
        id = int(line.split()[2])
        if len(line.split()) > 3:
            gauge_dict['Units'] = line.split()[3]
        time_tuple, nvalues = f.read_dateline()
        data, units = f.read_RavenFrame(nvalues=nvalues, header=False, time_tuple=time_tuple)
        # Rename based upon data type #TODO
        if gauge_dict['Type'] == 'HYDROGRAPH':
            data = data.rename(columns={0: 'QObs'})
        gauge_dict['Data'] = data
        self.obsgauges[id] = gauge_dict

    @property
    def nmetgauges(self):
        return len(self.metgauges)

    @property
    def nobsgauges(self):
        return len(self.obsgauges)

    def imet(self, i):
        """Accessor for meteorological gauge dataframe based on key index"""
        if i > self.nmetgauges:
            raise IndexError('Gauge index higher than number of gauges')
        else:
            return self.metgauges[list(self.metgauges.keys())[i]]['Data']

    def iobs(self, i):
        """Accessor for observation gauge dataframe based on key index"""
        if i > self.nobsgauges:
            raise IndexError('Gauge index higher than number of gauges')
        else:
            return self.obsgauges[list(self.obsgauges.keys())[i]]['Data']


# ----------------------------------------------------------------------------------------------------------------------#

class RavenRVH(RavenFile):

    def __init__(self, filename=None):
        self.subbasins = None
        self.hrus = None
        self.nsubbasins = 0
        self.nhrus = 0
        self.total_area = 0.0
        if filename:
            self.read(filename)

    def read(self, filename):
        with RavenFileReader(filename) as f:
            line = f.nexttag()
            while line:
                # Begin data type checks
                if self.cleantag(line) == 'SubBasins':
                    self.read_subbasins(line, f)
                elif self.cleantag(line) == 'HRUs':
                    self.read_HRUs(line, f)
                # Next line
                line = f.nexttag()

    def read_subbasins(self, line, f):
        self.subbasins, units = f.read_RavenFrame(id_col=True)
        self.nsubbasins = self.subbasins.shape[0]

    def read_HRUs(self, line, f):
        self.hrus, units = f.read_RavenFrame(id_col=True)
        self.nhrus = self.hrus.shape[0]
        self.total_area = self.hrus['AREA'].sum()
