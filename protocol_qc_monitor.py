from protocol_monitor import ProtMonitor, Monitor, PrintNotifier
from pyworkflow.em.protocol import ProtAlignMovies, ProtCTFMicrographs, ProtImportMovies  # noqa
from pyworkflow.em import ImageHandler
from pyworkflow.gui import getPILImage
import pyworkflow.protocol.params as params

from PIL import Image
import pyworkflow.utils as pwutils
import matplotlib.pyplot as plt
import os
from subprocess import call
import sqlite3
import json
import re
import string
import csv
import math
import numpy as np
import datetime
from collections import defaultdict

SQLITE_TO_TXT = {
    'Acquisition Magnification': 'Magnification',
    'Acquisition Voltage': 'Voltage',
    'Defocus U': 'DF1',
    'Defocus V': 'DF2',
    'Defocus Angle': 'Angast',
    'Gctf Cross Correlation': 'CCC',
    'Sampling Rate': 'Pixel Size',
}
TXT_FIELDS = [
    'Movie',
    'Micrograph',
    'Date',
    'Magnification',
    'Voltage',
    'Pixel Size',
    'Counts',
    'DF1',
    'DF2',
    'DF1-DF2',
    'Angast',
    'CCC',
    'Average Drift',
    'Maximum Drift',
]


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def standardize_label(label):
    '''
    Formats Scipion SQLite class names consistently
    '''
    # Remove camel case
    label = re.sub('(?!^)([A-Z]+)', r' \1', label).lower()

    # Replace punctuation with whitespace
    for p in string.punctuation:
        label = label.replace(p, ' ')

    # Remove extra spaces
    label = ' '.join(label.split())

    # Use title-style capitalization
    return label.title()


def get_absolute_drifts(x_shifts, y_shifts):
    return list(
        [math.sqrt(x ** 2 + y ** 2) for x, y in zip(x_shifts, y_shifts)])


class ProtQCSummary(ProtMonitor):
    _label = 'QC summary'

    def _defineParams(self, form):
        ProtMonitor._defineParams(self, form)

    def _validate(self):
        errors = []
        return errors

    def _insertAllSteps(self):
        self._insertFunctionStep('monitorStep')

    def monitorStep(self):
        monitor = QCMonitor(self, workingDir=self._getPath(),
                            samplingInterval=self.samplingInterval.get(),
                            monitorTime=100,
                            )
        monitor.addNotifier(PrintNotifier())
        monitor.loop()


class QCMonitor(Monitor):

    def __init__(self, protocol, **kwargs):

        Monitor.__init__(self, **kwargs)
        self.protocol = protocol
        self.project = protocol.getProject()
        self.run_count = 1

        self.protocol_fields = dict()  # Populated with Scipion SQLITE entries
        self.txt_fields = defaultdict(dict)  # Printed to CSV
        self.txt_output = os.path.join(
            self.workingDir,
            'extra',
            'compiled_qc_fields.txt',
        )

    def step(self):

        for protPointer in self.protocol.inputProtocols:
            prot = protPointer.get()

            if isinstance(prot, ProtAlignMovies):

                #  Create PNGs of micrographs
                if hasattr(prot, 'outputMicrographs'):
                    for mic in prot.outputMicrographs:
                        input_file = os.path.join(
                            self.project.path, mic.getFileName())
                        base_name = os.path.splitext(
                            os.path.basename(mic.getFileName()))[0]

                        self.set_micrograph_path(
                            input_file, base_name.split('_aligned_mic')[0])

                        output_file = os.path.join(
                            self.workingDir,
                            'extra',
                            base_name + '.png',
                        )
                        if not os.path.isfile(output_file):
                            self.generateMicImage(input_file, output_file)

                #  Create plots of offset values
                if hasattr(prot, 'outputMovies'):
                    for movie in prot.outputMovies:
                        base_name = os.path.splitext(
                            os.path.basename(movie.getFileName()))[0]

                        output_file = os.path.join(
                            self.workingDir,
                            'extra',
                            base_name + '.shift_plot.png'  # noqa
                        )
                        x_shifts, y_shifts = movie.getAlignment().getShifts()
                        if not os.path.isfile(output_file):
                            self.generateShiftPlot(
                                x_shifts, y_shifts, output_file)
                        self.set_average_drift(x_shifts, y_shifts, base_name)
                        self.set_maximum_drift(x_shifts, y_shifts, base_name)

                # Read SQLite database
                sqlite_file = prot._getPath('micrographs.sqlite')
                self.read_txt_fields_from_sqlite(sqlite_file)

            elif isinstance(prot, ProtCTFMicrographs):

                # Read SQLite database
                sqlite_file = prot._getPath('ctfs.sqlite')
                self.read_txt_fields_from_sqlite(sqlite_file)

                if hasattr(prot, 'outputCTF'):
                    for ctf in prot.outputCTF:

                        base_name = os.path.basename(
                            ctf.getMicrograph().getFileName()
                        ).split('_aligned_mic.mrc')[0]
                        self.set_defocus_delta(base_name)

                        psd_file = ctf.getPsdFile()
                        epa_file = os.path.splitext(psd_file)[0] + '_EPA.txt'

                        #  Generate PSD png
                        input_file = psd_file
                        output_file = os.path.join(
                            self.workingDir,
                            'extra',
                            psd_file.split('/')[-2] + '_PSD.png',
                        )
                        if not os.path.exists(output_file):
                            self.generateMicImage(input_file, output_file)

                        #  Generate EPA plot
                        input_file = epa_file
                        output_file = os.path.join(
                            self.workingDir,
                            'extra',
                            psd_file.split('/')[-2] + '_EPAplot.png',
                        )
                        if not os.path.exists(output_file):
                            self.generateEPAPlot(input_file, output_file)

            elif isinstance(prot, ProtImportMovies):
                for movie in prot.outputMovies:

                    movie_path = os.path.join(
                        self.project.path, movie.getFileName())
                    movie_base_name = \
                        os.path.splitext(os.path.basename(
                            movie.getFileName()))[0]

                    self.set_movie_counts(movie, movie_base_name)
                    self.set_movie_path(movie_path, movie_base_name)
                    self.set_movie_time(movie_path, movie_base_name)

                    #  Compile plot
                    exts = [
                        '_aligned_mic.png',
                        '.shift_plot.png',
                        '_aligned_mic_PSD.png',
                        '_aligned_mic_EPAplot.png',
                    ]
                    files = []
                    for e in exts:
                        files.append(os.path.join(
                            self.workingDir,
                            'extra',
                            movie_base_name + e,
                        ))

                    create = True
                    for f in files:
                        if not os.path.isfile(f):
                            create = False
                    if create:
                        result = Image.new("RGB", (1600, 400))
                        for i, f in enumerate(files):
                            img = Image.open(f)
                            img.thumbnail((400, 400), Image.ANTIALIAS)
                            x = i * 400
                            w, h = img.size
                            result.paste(img, (x, 0, x + w, h))
                        result.save(os.path.join(
                            self.workingDir,
                            'extra',
                            movie_base_name + '_quad.png'
                        ))

                # Read SQLite database
                sqlite_file = prot._getPath('movies.sqlite')
                self.read_txt_fields_from_sqlite(sqlite_file)

        self.write_txt_file()

    def write_txt_file(self):
        # self.info(self.txt_fields)
        with open(self.txt_output, 'w') as OUTPUT:
            writer = csv.DictWriter(OUTPUT, fieldnames=TXT_FIELDS)
            writer.writeheader()
            for key, field_dict in sorted(
                    self.txt_fields.items(), key=lambda x: x[0]):
                writer.writerow(field_dict)

    def set_movie_path(self, movie_path, base_name):
        self.txt_fields[base_name]['Movie'] = os.path.realpath(movie_path)

    def set_movie_time(self, movie_path, base_name):
        real_path = os.path.realpath(movie_path)
        t = os.path.getmtime(real_path)
        self.txt_fields[base_name]['Date'] = datetime.datetime.fromtimestamp(t)

    def set_movie_counts(self, movie, base_name):
        dose_per_frame = max(0, movie.getAcquisition().getDosePerFrame())
        initial_dose = max(0, movie.getAcquisition().getDoseInitial())
        frames = movie.getNumberOfFrames()

        counts = float(initial_dose) + float(dose_per_frame) * frames
        self.txt_fields[base_name]['Counts'] = counts

    def set_micrograph_path(self, micrograph_path, base_name):
        self.txt_fields[base_name]['Micrograph'] = micrograph_path

    def set_average_drift(self, x_shifts, y_shifts, base_name):
        drifts = get_absolute_drifts(x_shifts, y_shifts)
        self.txt_fields[base_name]['Average Drift'] = np.average(drifts)

    def set_maximum_drift(self, x_shifts, y_shifts, base_name):
        drifts = get_absolute_drifts(x_shifts, y_shifts)
        self.txt_fields[base_name]['Maximum Drift'] = max(drifts)

    def set_defocus_delta(self, base_name):
        try:
            df_1 = self.txt_fields[base_name]['DF1']
            df_2 = self.txt_fields[base_name]['DF2']
        except KeyError:
            pass
        else:
            self.txt_fields[base_name]['DF1-DF2'] = df_1 - df_2

    def read_txt_fields_from_sqlite(self, sqlite_file):
        connection = sqlite3.connect(sqlite_file)
        connection.row_factory = dict_factory
        cursor = connection.cursor()

        col_name_to_label = dict()
        for row in cursor.execute('SELECT * FROM Classes'):
            label_property = standardize_label(row['label_property'])
            col_name_to_label[row['column_name']] = label_property

        try:
            for row in cursor.execute('SELECT * FROM Objects'):

                row_keys = row.keys()
                for key in row_keys:
                    if key in col_name_to_label:
                        row[col_name_to_label[key]] = row.pop(key)

                sqlite_base = os.path.basename(sqlite_file)

                if sqlite_base == 'movies.sqlite':
                    base_name = (os.path
                                   .basename(row['Filename'])
                                   .split('.mrcs')[0])
                elif sqlite_base == 'micrographs.sqlite':
                    base_name = (os.path
                                   .basename(row['Filename'])
                                   .split('_aligned_mic.mrc')[0])
                elif sqlite_base == 'ctfs.sqlite':
                    base_name = (os.path
                                   .basename(row['Mic Obj Filename'])
                                   .split('_aligned_mic.mrc')[0])

                if base_name not in self.protocol_fields:
                    self.protocol_fields[base_name] = dict()

                self.protocol_fields[base_name].update(row)

                for key, value in row.items():
                    if key in SQLITE_TO_TXT:
                        txt_key = SQLITE_TO_TXT[key]
                        self.txt_fields[base_name][txt_key] = value

        except sqlite3.OperationalError:
            pass

    def read_to_protocol_fields(self, sqlite_file):

        connection = sqlite3.connect(sqlite_file)
        connection.row_factory = dict_factory
        cursor = connection.cursor()

        col_name_to_label = dict()

        for row in cursor.execute('SELECT * FROM Classes'):
            label_property = standardize_label(row['label_property'])
            col_name_to_label[row['column_name']] = label_property

        try:
            for row in cursor.execute('SELECT * FROM Objects'):

                row_keys = row.keys()
                for key in row_keys:
                    if key in col_name_to_label:
                        row[col_name_to_label[key]] = row.pop(key)

                sqlite_base = os.path.basename(sqlite_file)

                if sqlite_base == 'movies.sqlite':
                    base_name = (os.path
                                   .basename(row['Filename'])
                                   .split('.mrcs')[0])
                elif sqlite_base == 'micrographs.sqlite':
                    base_name = (os.path
                                   .basename(row['Filename'])
                                   .split('_aligned_mic.mrc')[0])
                elif sqlite_base == 'ctfs.sqlite':
                    base_name = (os.path
                                   .basename(row['Mic Obj Filename'])
                                   .split('_aligned_mic.mrc')[0])

                if base_name not in self.protocol_fields:
                    self.protocol_fields[base_name] = dict()

                self.protocol_fields[base_name].update(row)

        except sqlite3.OperationalError:
            pass

    def generateMicImage(self, input_file, output_file=None):
        if not output_file:
            output_file = os.path.splitext(input_file)[0] + '.png'
        img = ImageHandler().createImage()
        img.read(input_file)
        pimg = getPILImage(img)
        pwutils.makeFilePath(output_file)
        pimg.save(output_file, "PNG")

    def generateShiftPlot(self, cume_x_shifts, cume_y_shifts, output_file):
        x_shifts = []
        y_shifts = []
        for i in range(1, len(cume_x_shifts)):
            x_shifts.append(cume_x_shifts[i] - cume_x_shifts[i - 1])
        for j in range(1, len(cume_y_shifts)):
            y_shifts.append(cume_y_shifts[j] - cume_y_shifts[j - 1])

        width = 1 / 1.5

        f, axarr = plt.subplots(2, sharex=True)

        axarr[0].bar(range(len(x_shifts)), x_shifts, width, color='blue')
        axarr[0].set_title('X axis shifts (non-cumulative)')
        axarr[0].set_ylabel('Shift')

        axarr[1].bar(range(len(y_shifts)), y_shifts, width, color='blue')
        axarr[1].set_title('Y axis shifts (non-cumulative)')
        axarr[1].set_xlabel('Frame')
        axarr[1].set_ylabel('Shift')

        f.set_size_inches(8, 8)
        plt.savefig(output_file)
        plt.clf()

    def generateEPAPlot(self, input_file, output_file):

        def _plot_subset(axis, resolution_list, ctf_sim_list, epa_ln_f_bg_list,
                         ccc_list, res_max=float('inf'), res_min=float('-inf')):

            plot_ccc_lists = {
                1.0: [],
                0.8: [],
                0.5: [],
            }
            res_limits = {
                0.8: None,
                0.5: None,
            }
            current_list = plot_ccc_lists[1.0]

            plot_resolution_list = []
            plot_ctf_sim_list = []
            plot_epa_ln_f_bg_list = []

            for resolution, ctf_sim, epa_ln_f_bg, ccc in zip(
                    resolution_list, ctf_sim_list, epa_ln_f_bg_list, ccc_list):

                if resolution <= res_max and resolution >= res_min:

                    plot_resolution_list.append(1.0 / resolution)
                    plot_ctf_sim_list.append(ctf_sim)
                    plot_epa_ln_f_bg_list.append(epa_ln_f_bg)

                    if not res_limits[0.8]:
                        if ccc <= 0.5:
                            res_limits[0.5] = resolution
                            res_limits[0.8] = resolution
                            current_list.append({
                                'resolution': 1.0 / resolution,
                                'ccc': ccc,
                            })
                            current_list = plot_ccc_lists[0.5]
                        elif ccc <= 0.8:
                            res_limits[0.8] = resolution
                            current_list.append({
                                'resolution': 1.0 / resolution,
                                'ccc': ccc,
                            })
                            current_list = plot_ccc_lists[0.8]
                    elif not res_limits[0.5]:
                        if ccc <= 0.5:
                            res_limits[0.5] = resolution
                            current_list.append({
                                'resolution': 1.0 / resolution,
                                'ccc': ccc,
                            })
                            current_list = plot_ccc_lists[0.5]
                            if not res_limits[0.8]:
                                res_limits[0.8] = resolution
                    current_list.append({
                        'resolution': 1.0 / resolution,
                        'ccc': ccc,
                    })

            epa_max = max(plot_epa_ln_f_bg_list)
            epa_min = min(plot_epa_ln_f_bg_list)
            epa_diff = epa_max - epa_min

            epa_norm = []
            for val in plot_epa_ln_f_bg_list:
                epa_norm.append(
                    (val - epa_min) / epa_diff
                )

            axis.plot(
                plot_resolution_list,
                plot_ctf_sim_list,
                color='gray',
                label='CTF Sim.',
                alpha=0.7,
            )
            axis.plot(
                plot_resolution_list,
                epa_norm,
                color='blue',
                label='BG-Corr. EPA'
            )

            for limit, color in zip([(1.0, 0.8), (0.8, 0.5), (0.5, -1.0)],
                                    ['green', 'orange', 'red']):

                axis.plot(
                    [x['resolution'] for x in plot_ccc_lists[limit[0]]],
                    [x['ccc'] for x in plot_ccc_lists[limit[0]]],
                    linewidth=2,
                    color=color,
                    label='{} >= CCC > {}'.format(str(limit[0]), str(limit[1])),
                )

            if res_limits[0.8]:
                axis.axvline(1 / res_limits[0.8], color='orange')
            if res_limits[0.5]:
                axis.axvline(1 / res_limits[0.5], color='red')

            axis.set_xlabel('Resolution (1 / A)')
            axis.set_ylabel('Correlation')

            axis.set_xlim(min(plot_resolution_list), max(plot_resolution_list))
            axis.set_ylim([-0.2, 1.2])

            return res_limits

        resolution_list = []
        ctf_sim_list = []
        epa_ln_f_bg_list = []
        ccc_list = []

        with open(input_file) as f:
            next(f)
            for line in f:
                resolution, ctf_sim, epa_ln_f, epa_ln_f_bg, ccc = \
                    [float(val) for val in line.strip().split()]

                resolution_list.append(resolution)
                ctf_sim_list.append(ctf_sim)
                epa_ln_f_bg_list.append(epa_ln_f_bg)
                ccc_list.append(ccc)

        f, axarr = plt.subplots(4)
        f.set_size_inches(8, 8)

        res_limits = _plot_subset(axarr[0], resolution_list, ctf_sim_list,
                                  epa_ln_f_bg_list, ccc_list)
        axarr[0].set_title(
            'Resolution limits: {} A at 0.8 CCC and {} A at 0.5 CCC'.format(
                str(round(res_limits[0.8], 2)),
                str(round(res_limits[0.5], 2)),
            ))

        _plot_subset(axarr[1], resolution_list, ctf_sim_list, epa_ln_f_bg_list,
                     ccc_list, res_min=10)
        axarr[1].set_title('20 A to 10 A')
        _plot_subset(axarr[2], resolution_list, ctf_sim_list, epa_ln_f_bg_list,
                     ccc_list, res_max=10, res_min=5)
        axarr[2].set_title('10 A to 5 A')
        _plot_subset(axarr[3], resolution_list, ctf_sim_list, epa_ln_f_bg_list,
                     ccc_list, res_max=5, res_min=2)
        axarr[3].set_title('5 A to 2 A')

        plt.tight_layout()
        plt.savefig(output_file)
        plt.clf()
