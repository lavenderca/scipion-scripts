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


def checkRemoteFile(host, path):
    status = call([
        'ssh', host, 'test', '-f', path,
    ])
    return status == 0


class ProtQCSummary(ProtMonitor):
    _label = 'QC summary'

    def _defineParams(self, form):
        ProtMonitor._defineParams(self, form)

        form.addSection('Movie file transfer')

        form.addParam('transfer', params.BooleanParam, default=False,
                      label='Transfer movie files?')

        form.addParam('transferMethod', params.EnumParam, default=0,
                      choices=['scp', 'bbcp'], condition='transfer',
                      label='Method to transfer files')

        form.addParam('compress', params.BooleanParam, default=True,
                      condition='transfer',
                      label='Compress before transfer?')

        form.addParam('destinationHost', params.StringParam, default=None,
                      condition='transfer',
                      label='Destination host')

        form.addParam('destinationDirectory', params.StringParam, default=None,
                      condition='transfer',
                      label='Destination directory')

        form.addParam('destinationUser', params.StringParam, default=None,
                      condition='transfer',
                      label='Destination user name')

    def _validate(self):
        errors = []

        return errors

    def _insertAllSteps(self):
        self._insertFunctionStep('monitorStep')

    def monitorStep(self):
        monitor = QCMonitor(self, workingDir=self._getPath(),
                            samplingInterval=self.samplingInterval.get(),
                            monitorTime=100,

                            compress=self.compress.get(),
                            transfer=self.transfer.get(),
                            transferMethod=self.transferMethod.get(),
                            destinationHost=self.destinationHost.get(),
                            destinationDirectory=self.destinationDirectory.get(),  # noqa
                            destinationUser=self.destinationUser.get(),
                            )

        monitor.addNotifier(PrintNotifier())
        monitor.loop()


class QCMonitor(Monitor):
    def __init__(self, protocol, **kwargs):
        Monitor.__init__(self, **kwargs)
        self.protocol = protocol
        self.project = protocol.getProject()
        self.run_count = 1

        self.compress = kwargs['compress']
        self.transfer = kwargs['transfer']

        if kwargs['transferMethod'] == 0:
            self.transferMethod = 'scp'
        elif kwargs['transferMethod'] == 1:
            self.transferMethod = 'bbcp'

        self.destinationHost = kwargs['destinationHost']

        self.destinationDirectory = kwargs['destinationDirectory']
        if not self.destinationDirectory:
            self.destinationDirectory = ''

        self.destinationUser = kwargs['destinationUser']

    def step(self):

        for protPointer in self.protocol.inputProtocols:
            prot = protPointer.get()

            if isinstance(prot, ProtAlignMovies):

                #  Create PNGs of micrographs
                if hasattr(prot, 'outputMicrographs'):
                    for mic in prot.outputMicrographs:
                        input_file = os.path.join(
                            self.project.path, mic.getFileName())
                        output_file = os.path.join(
                            self.workingDir,
                            'extra',
                            os.path.splitext(os.path.basename(mic.getFileName()))[0] + '.png',  # noqa
                        )
                        if not os.path.isfile(output_file):
                            self.generateMicImage(input_file, output_file)

                #  Create plots of offset values
                if hasattr(prot, 'outputMovies'):
                    for movie in prot.outputMovies:
                        output_file = os.path.join(
                            self.workingDir,
                            'extra',
                            os.path.splitext(os.path.basename(movie.getFileName()))[0] + '.shift_plot.png'  # noqa
                        )
                        x_shifts, y_shifts = movie.getAlignment().getShifts()
                        if not os.path.isfile(output_file):
                            self.generateShiftPlot(
                                x_shifts, y_shifts, output_file)

            elif isinstance(prot, ProtCTFMicrographs):

                if hasattr(prot, 'outputCTF'):
                    for ctf in prot.outputCTF:
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

        for protPointer in self.protocol.inputProtocols:
            prot = protPointer.get()

            if isinstance(prot, ProtImportMovies):
                for movie in prot.outputMovies:
                    movie_base_name = \
                        os.path.splitext(os.path.basename(
                            movie.getFileName()))[0]

                    #  Transfer movie file:
                    if self.transfer:
                        transfer_file = os.path.join(
                            os.getcwd(),
                            movie.getFileName(),
                        )

                        #  Compress movie file
                        if self.compress:
                            compressed_movie_file = os.path.join(
                                os.getcwd(),
                                self.workingDir,
                                'extra',
                                movie_base_name + '.gz',
                            )
                            if not os.path.isfile(compressed_movie_file):
                                with open(compressed_movie_file, 'w') as OUT:
                                    call([
                                        'gzip', '-c',
                                        movie.getFileName(),
                                    ], stdout=OUT)
                            transfer_file = compressed_movie_file

                        #  Perform transfer
                        host = self.destinationUser + \
                            '@' + self.destinationHost
                        path = os.path.join(self.destinationDirectory,
                            os.path.basename(transfer_file))

                        if not checkRemoteFile(host, path):
                            if self.transferMethod == 'scp':
                                call([
                                    'scp',
                                    transfer_file,
                                    self.destinationUser + '@' +
                                    self.destinationHost + ':' +
                                    self.destinationDirectory,
                                ])
                            elif self.transferMethod == 'bbcp':
                                self.info(self.workingDir)
                                self.info(os.getcwd())
                                self.info(transfer_file)

                                if self.destinationDirectory == '':
                                    destination_dir = '.'
                                else:
                                    destination_dir = self.destinationDirectory

                                call([
                                    'bbcp', '-w', '8m', '-s', '16',
                                    transfer_file,
                                    self.destinationUser + '@' +
                                    self.destinationHost + ':' +
                                    destination_dir,
                                ])

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
                        result = Image.new("RGB", (800, 800))
                        for i, f in enumerate(files):
                            img = Image.open(f)
                            img.thumbnail((400, 400), Image.ANTIALIAS)
                            x = i // 2 * 400
                            y = i % 2 * 400
                            w, h = img.size
                            result.paste(img, (x, y, x + w, y + h))
                        result.save(os.path.join(
                            self.workingDir,
                            'extra',
                            movie_base_name + '_quad.png'
                        ))

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

        plt.savefig(output_file)
        plt.clf()

    def generateEPAPlot(self, input_file, output_file):
        resolution_list = []
        ctf_sim_list = []
        epa_ln_f_bg_list = []

        ccc_lists = {
            1.0: [],
            0.8: [],
            0.5: [],
        }
        res_limits = {
            0.8: None,
            0.5: None,
        }
        ccc_list = ccc_lists[1.0]

        with open(input_file) as f:
            next(f)
            for line in f:
                resolution, ctf_sim, epa_ln_f, epa_ln_f_bg, ccc = \
                    [float(val) for val in line.strip().split()]

                resolution_list.append(1.0 / resolution)
                ctf_sim_list.append(ctf_sim)
                epa_ln_f_bg_list.append(epa_ln_f_bg)

                if not res_limits[0.8]:
                    if ccc <= 0.5:
                        res_limits[0.5] = resolution
                        res_limits[0.8] = resolution
                        ccc_list.append({
                            'resolution': 1.0 / resolution,
                            'ccc': ccc,
                        })
                        ccc_list = ccc_lists[0.5]
                    elif ccc <= 0.8:
                        res_limits[0.8] = resolution
                        ccc_list.append({
                            'resolution': 1.0 / resolution,
                            'ccc': ccc,
                        })
                        ccc_list = ccc_lists[0.8]
                elif not res_limits[0.5]:
                    if ccc <= 0.5:
                        res_limits[0.5] = resolution
                        ccc_list.append({
                            'resolution': 1.0 / resolution,
                            'ccc': ccc,
                        })
                        ccc_list = ccc_lists[0.5]
                        if not res_limits[0.8]:
                            res_limits[0.8] = resolution
                ccc_list.append({
                    'resolution': 1.0 / resolution,
                    'ccc': ccc,
                })

        plt.plot(resolution_list, ctf_sim_list, color='black', label='CTF Sim.')
        plt.plot(
            resolution_list, epa_ln_f_bg_list, color='blue', label='BG-Corr. EPA')

        for limit, color in zip([(1.0, 0.8), (0.8, 0.5), (0.5, -1.0)],
                                ['green', 'orange', 'red']):
            plt.plot(
                [x['resolution'] for x in ccc_lists[limit[0]]],
                [x['ccc'] for x in ccc_lists[limit[0]]],
                linewidth=2,
                color=color,
                label='{} >= CCC > {}'.format(str(limit[0]), str(limit[1])),
            )

        plt.title('Resolution limits: {} A at 0.8 and {} A at 0.5'.format(
            str(round(res_limits[0.8], 2)),
            str(round(res_limits[0.5], 2)),
        ))
        plt.axvline(1 / res_limits[0.8], color='orange')
        plt.axvline(1 / res_limits[0.5], color='red')

        plt.xlabel('Resolution (1 / A)')
        plt.ylabel('Correlation')

        plt.xlim(min(resolution_list), max(resolution_list))
        plt.ylim([-0.2, 1.2])

        plt.legend()

        plt.savefig(output_file)
        plt.clf()
