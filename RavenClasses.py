import datetime as dt
import pandas as pd
from os import path

# ----------------------------------------------------------------------------------------------------------------------#

class RavenFileReader(object):
    """Generic text file reader with special methods for working with Raven Hydrological Modelling Framework
    (raven.uwaterloo.ca) input files.
    Methods `nexttag()`, `read_dateline()` and `read_RavenFrame()` should be able to get you through just
    about anything in a Raven input file.

    TODO - use pathlib Path() explicitly instead of str for filepaths?

    Attributes
    ----------
    filename : str (or Path)
        Filename, with path, of file being read
    path : str
        Path to directory containing the file opened, used when opening :RedirectToFile files
    fileobject : IO
        Connection to file created with open()
    _backburner : list
        Private list that stores the file(s) from which :RedirectToFile was called, used allow
        for faux-recursive file reading

    Methods
    -------
    eof_check()
        Checks for end of file (EOF) by requesting a certain number of bytes and seeing if they are returned
    nextline()
        Returns next line, pre-stripped
    nexttag()
        Returns the next Raven tag line (starts with :), with smart faux-recursive handling of file
        redirects. False if EOF.
    read_dateline()
        Parses a Raven dateline: start date/time, time step size, number of values in file
    skiplines(lines)
        Skips specified number of lines in file
    comma_detector()
        Attempts to determine if Raven line is delimited by commas (as opposed to spaces/tabs)
    read_RavenFrame(nvalues, header, names, id_col, time_tuple, na_values)
        Reads a raven property/attribute table into a Pandas DataFrame
    get_datadist()
        Finds the distance to the next tag (:) or a blank line. Useful to determine table/data lengths
    """

    def __init__(self, filename):
        self.filename = filename
        self.path = path.dirname(filename)
        self.fileobject = None
        self._backburner = []

    def __enter__(self):
        """For using with RavenFileReader()"""
        self.fileobject = open(self.filename, 'r')
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """ Runs when with RavenFileReader() statement is exited"""
        self.fileobject.close()

    def eof_check(self) -> bool:
        """
        True if end of file (EOF) has been reached, false if otherwise. Checks if there was a parent file (now on
        the so-called back burner) that redirected to the file. If so, switches back to that file and continues.
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
                # TODO: what if it is the end of the back burner file? Is that handled?
            else:
                eof = True
        else:
            self.fileobject.seek(curr_pos)
        return eof

    def nextline(self) -> str:
        """ Returns next line, as a stripped string"""
        return self.fileobject.readline().strip()

    def nexttag(self) -> str:
        """Reads through lines of file (or RedirectToFile) looking for the next line that starts with :,
         which generally denotes a Raven command or attribute. Will switch to RedirectToFile if command
         encountered
         :return str of line, or False if EOF is reached
         """
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

    def read_dateline(self) -> ((dt.datetime, dt.timedelta), int):
        """Parses a Raven dateline: start date/time, time step size, number of values in file
        :return (start datetime, time step size), number of values
        """
        line = self.nextline().split()
        start = dt.datetime.strptime('{} {}'.format(line[0], line[1]), '%Y-%m-%d  %H:%M:%S')
        delta = dt.timedelta(days=float(line[2]))
        nvalues = int(line[3])
        return (start, delta), nvalues

    def skiplines(self, lines: int):
        """Skips lines in current file
        :type lines: int number of lines to skip
        """
        for i in range(0, lines):
            self.fileobject.readline()

    def comma_detector(self) -> bool:
        """Attempts to determine if Raven line is delimited by commas (as opposed to spaces/tabs)
        :return True if comma delimited, false if not
        """
        curr_pos = self.fileobject.tell()
        line = self.nextline()
        comma = False
        # A bold presumption, perhaps
        if ',' in line:
            comma = True
        self.fileobject.seek(curr_pos)
        return comma

    def read_RavenFrame(self, nvalues: int = None, header: bool = True, names: list = None,
                        id_col: bool = False, time_tuple: tuple = None, na_values: str = '-1.2345') \
            -> (pd.DataFrame, list):
        """
        Reads a raven property/attribute table into a Pandas DataFrame
        TODO: smarter way to return/store units??
        :param nvalues: int Number of values in table, if None will attempt autodetect (default: None)
        :param header: bool of whether table has a attributes/parameters & units header (default: True)
        :param names: list of attribute/parameter names, will attempt to infer if None (default: None)
        :param id_col: bool whether a numeric ID column, not present in table header, exists (default: False)
        :param time_tuple: tuple of (datetime, timedelta) (e.g. from RavenFileReader.read_dateline()) used to determine
        dates of time series. If None is passed it is assumed to not be a time series (default: None)
        :param na_values: str of value that represents NA (default: '-1.2345')
        :return: DataFrame of values, list of units
        """
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
        """ Finds the distance to the next tag (:) or a blank line. Useful to determine table/data lengths
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
    """ Base Class for other RavenFile classes to inherit from."""

    @staticmethod
    def cleantag(line: str):
        """
        Given a typical Raven "tag" starting with `:` will return only the text after the colon
        :param line str of text from file
        :return: str tag
        """
        cleaned = line.strip().replace(':', '')
        if len(cleaned.split()) > 1:
            cleaned = cleaned.split()[0]
        return cleaned


# ----------------------------------------------------------------------------------------------------------------------#

class RavenRVT(RavenFile):
    """Class for reading in Raven Time Series Input File

    Attributes
    ----------
    metgauges : dict
        Meteorological gauge latitude, longitude, elevation, type, data, and units, keyed by gauge name
    obsgauges : dict
        Observation gauge type, units, and data keyed by gauge name

    Methods
    -------
    read(filename)
        Reads RVT file
    read_metgauge(line, RavenFileReader)
        Reads meteorological gauge entry in RVT file, starting at line = :Gauge [name]
    read_obsgauge(line, RavenFileReader)
        Reads observation gauge entry starting at line = :ObservationData [data_type] [basin_ID or HRU_ID] {units}
    nmetgauges
        Property, number of meteorological gauges in RVT file
    nobsgauges
        Property, number of observation gauges in RVT file
    imet(i)
        Accessor for meteorological gauge dataframe i
    iobs(i)
        Accessor for observation gauge dataframe i
    """
    def __init__(self, filename=None):
        """
        Reads Raven Time Series Input File (.rvt)
        :param filename: filename: RVT file path
        """
        self.metgauges = {}
        self.obsgauges = {}
        if filename:
            self.read(filename)

    def read(self, filename):
        """
        Reads Raven Time Series Input File (.rvt)
        :param filename: RVT file path
        """
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
        """
        Reads meteorological gauge entry in RVT file, starting at line = :Gauge [name]
        :param line: str, first line containing [name]
        :param f: RavenFileReader object
        """
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
                gauge_dict['Type'] = line.split()[1]
                gauge_dict['Units'] = line.split()[2]
                time_tuple, nvalues = f.read_dateline()
                data, units = f.read_RavenFrame(nvalues=nvalues, header=False, time_tuple=time_tuple)
                gauge_dict['Data'] = data
                self.metgauges[name] = gauge_dict
            # Next line
            line = f.nexttag()

    def read_obsgauge(self, line: str, f: RavenFileReader):
        """Reads observation gauge entry starting at line = :ObservationData [data_type] [basin_ID or HRU_ID] {units}

        :param line: str, first line containing [data_type] [basin_ID or HRU_ID] {units}
        :param f: RavenFileReader object
        """
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
        """Total meteorological gauges read"""
        return len(self.metgauges)

    @property
    def nobsgauges(self):
        """Total observation gauges read"""
        return len(self.obsgauges)

    def imet(self, i) -> pd.DataFrame:
        """Accessor for meteorological gauge dataframe based on key index
        :param i: gauge index
        :return: Pandas DataFrame
        """
        if i > self.nmetgauges:
            raise IndexError('Gauge index higher than number of gauges')
        else:
            return self.metgauges[list(self.metgauges.keys())[i]]['Data']

    def iobs(self, i) -> pd.DataFrame:
        """Accessor for observation gauge dataframe based on key index
        :param i: gauge index
        :return: Pandas DataFrame
        """
        if i > self.nobsgauges:
            raise IndexError('Gauge index higher than number of gauges')
        else:
            return self.obsgauges[list(self.obsgauges.keys())[i]]['Data']


# ----------------------------------------------------------------------------------------------------------------------#

class RavenRVH(RavenFile):
    """Class for reading in Raven HRU/Basin Definition File (.rvh)

    Attributes
    ----------
    subbasins : DataFrame
        Subbasin data table [ID, NAME, DOWNSTREAM_ID, PROFILE, REACH_LENGTH, GAUGED]
    hrus : DataFrame
        HRU data table [AREA,ELEVATION,LATITUDE,LONGITUDE,BASIN_ID,LAND_USE_CLASS,VEG_CLASS,
        SOIL_PROFILE,AQUIFER_PROFILE,TERRAIN_CLASS,SLOPE,ASPECT]
    nsubbasins : int
        Number of subbasins
    nhrus : int
        Number of HRUs
    total_area : float
        Total area in Raven model

    Methods
    -------
    read(filename)
        Reads RVH file
    read_subbasins(RavenFileReader)
        Reads subbasin table starting at :SubBasins
    read_HRUs(RavenFileReader)
        Reads HRU table starting at :HRUs
    """
    def __init__(self, filename=None):
        """Reads Raven HRU/Basin Definition File (.rvh)
        :param filename: RVH file path
        """
        self.subbasins = None
        self.hrus = None
        self.nsubbasins = 0
        self.nhrus = 0
        self.total_area = 0.0
        if filename:
            self.read(filename)

    def read(self, filename):
        """Reads Raven HRU/Basin Definition File (.rvh)
        :param filename: RVH file path
        """
        with RavenFileReader(filename) as f:
            line = f.nexttag()
            while line:
                # Begin data type checks
                if self.cleantag(line) == 'SubBasins':
                    self.read_subbasins(f)
                elif self.cleantag(line) == 'HRUs':
                    self.read_HRUs(f)
                # Next line
                line = f.nexttag()

    def read_subbasins(self, f: RavenFileReader):
        """Reads subbasin table starting at :SubBasins
        :param f: RavenFileReader object
        """
        self.subbasins, units = f.read_RavenFrame(id_col=True)
        self.nsubbasins = self.subbasins.shape[0]

    def read_HRUs(self, f: RavenFileReader):
        """Reads HRU table starting at :HRUs
        :param f: RavenFileReader object
        """
        self.hrus, units = f.read_RavenFrame(id_col=True)
        self.nhrus = self.hrus.shape[0]
        self.total_area = self.hrus['AREA'].sum()
