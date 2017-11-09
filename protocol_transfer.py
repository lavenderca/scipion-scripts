import os

import pexpect
from subprocess import call
from Tkinter import Tk, Label, Button, Entry

import pyworkflow.protocol.params as params
import pyworkflow.utils as pwutils
from protocol_monitor import ProtMonitor, Monitor, PrintNotifier
from pyworkflow.em.protocol import ProtImportMovies


def checkRemoteFile(user, host, path, password=None):
    child = pexpect.spawn(' '.join([
        'ssh', '{}@{}'.format(user, host), 'test', '-f', path,
    ]))
    if child.expect(['password:', pexpect.EOF]) == 0:
        child.sendline(password)
        child.expect(pexpect.EOF)
    child.close()
    return child.exitstatus == 0


def check_password(user, host, password):
    child = pexpect.spawn(' '.join([
        'ssh', '{}@{}'.format(user, host), 'test',
    ]))
    child.expect('password:')
    child.sendline(password)
    return child.expect(['password:', pexpect.EOF]) == 1


class PasswordPrompt:

    def __init__(self, master, w=360, h=100, retry=False):

        self.master = master
        if retry:
            master.title('Password incorrect. Try again.')
        else:
            master.title('Password prompt')

        self.password_label = Label(master, text='Password:')
        self.password_label.pack(pady=(10, 0))

        self.password_entry = Entry(master, show='*', width=36)
        self.password_entry.pack()

        self.close_button = Button(master, text='Submit', command=self.submit)
        self.close_button.pack(pady=10)

        self.password = None

        ws = master.winfo_screenwidth() # width of the screen
        hs = master.winfo_screenheight() # height of the screen

        master.geometry('{}x{}+{}+{}'.format(
            str(w),
            str(h),
            str((ws / 2) - (w / 2)),
            str((hs / 2) - (h / 2)),
        ))

    def submit(self):

        if self.password_entry.get():
            self.password = self.password_entry.get()

        self.master.destroy()


class ProtTransfer(ProtMonitor):
    _label = 'Transfer'

    def _defineParams(self, form):
        ProtMonitor._defineParams(self, form)

        form.addParam('transferMethod', params.EnumParam, default=0,
                      choices=['scp', 'bbcp'], label='Transfer method')

        form.addParam('compress', params.BooleanParam, default=True,
                      label='Compress before transfer?')

        form.addParam('destinationHost', params.StringParam, default=None,
                      label='Destination host')

        form.addParam('destinationDirectory', params.StringParam, default=None,
                      label='Destination directory')

        form.addParam('destinationUser', params.StringParam, default=None,
                      label='Destination user name')

    def _validate(self):
        errors = []
        return errors

    def _insertAllSteps(self):
        self._insertFunctionStep('monitorStep')

    def monitorStep(self):

        user = self.destinationUser.get()
        host = self.destinationHost.get()

        monitor = TransferMonitor(
            self, workingDir=self._getPath(),
            samplingInterval=self.samplingInterval.get(),
            monitorTime=100,
            compress=self.compress.get(),
            transferMethod=self.transferMethod.get(),
            destinationHost=host,
            destinationDirectory=self.destinationDirectory.get(),
            destinationUser=user,
        )

        get_password = False
        check_ssh = pexpect.spawn(' '.join([
            'ssh',
            '{}@{}'.format(
                user,
                host,
            ),
            'test',
        ]))
        _index = check_ssh.expect(['(yes/no)', 'password:', pexpect.EOF])
        if _index == 0:
            check_ssh.sendline('yes')
            if check_ssh.expect(['password:', pexpect.EOF]) == 0:
                get_password = True
        elif _index == 1:
            get_password = True
        check_ssh.close()

        if get_password:
            root = Tk()
            prompt = PasswordPrompt(root)
            root.mainloop()
            password = prompt.password

            while check_password(user, host, password) == False:
                root = Tk()
                prompt = PasswordPrompt(root, retry=True)
                root.mainloop()
                password = prompt.password

            monitor.password = prompt.password

        monitor.addNotifier(PrintNotifier())
        monitor.loop()


class TransferMonitor(Monitor):
    def __init__(self, protocol, **kwargs):

        Monitor.__init__(self, **kwargs)
        self.protocol = protocol
        self.project = protocol.getProject()
        self.run_count = 1

        self.compress = kwargs['compress']
        if kwargs['transferMethod'] == 0:
            self.transferMethod = 'scp'
        elif kwargs['transferMethod'] == 1:
            self.transferMethod = 'bbcp'

        self.destinationHost = kwargs['destinationHost']
        self.destinationDirectory = kwargs['destinationDirectory']
        if not self.destinationDirectory:
            self.destinationDirectory = ''
        self.destinationUser = kwargs['destinationUser']

        self.password = None

    def step(self):

        for protPointer in self.protocol.inputProtocols:
            prot = protPointer.get()

            if isinstance(prot, ProtImportMovies):
                for movie in prot.outputMovies:
                    movie_base_name = \
                        os.path.splitext(os.path.basename(
                            movie.getFileName()))[0]

                    #  Transfer movie file:
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
                            self.info('Compressing {} by gzip.'.format(
                                os.path.basename(movie.getFileName()),
                            ))
                            with open(compressed_movie_file, 'w') as OUT:
                                call([
                                    'gzip', '-c',
                                    movie.getFileName(),
                                ], stdout=OUT)
                        transfer_file = compressed_movie_file

                    #  Perform transfer
                    user = self.destinationUser
                    host = self.destinationHost
                    path = os.path.join(self.destinationDirectory,
                        os.path.basename(transfer_file))

                    if not checkRemoteFile(user, host, path, self.password):

                        if self.destinationDirectory == '':
                            destination_dir = '.'
                        else:
                            destination_dir = self.destinationDirectory

                        if self.transferMethod == 'scp':
                            self.info('Sending {} to {} by scp.'.format(
                                os.path.basename(transfer_file),
                                self.destinationHost,
                            ))
                            child = pexpect.spawn(' '.join([
                                'scp',
                                transfer_file,
                                self.destinationUser + '@' +
                                self.destinationHost + ':' +
                                destination_dir,
                            ]))
                            if child.expect(['password:', pexpect.EOF],
                                            timeout=None) == 0:
                                child.sendline(self.password)
                                child.expect(pexpect.EOF)
                            child.close()

                        elif self.transferMethod == 'bbcp':
                            self.info('Sending {} to {} by bbcp.'.format(
                                os.path.basename(transfer_file),
                                self.destinationHost,
                            ))
                            child = pexpect.spawn(' '.join([
                                'bbcp', '-w', '8m', '-s', '16',
                                transfer_file,
                                self.destinationUser + '@' +
                                self.destinationHost + ':' +
                                destination_dir,
                            ]))
                            if child.expect(['password:', pexpect.EOF],
                                            timeout=None) == 0:
                                child.sendline(self.password)
                                child.expect(pexpect.EOF)
                            child.close()
