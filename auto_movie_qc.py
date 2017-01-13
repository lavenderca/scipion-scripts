import os
import argparse
import time

from multiprocessing import Pool

from pyworkflow.manager import Manager
from pyworkflow.em.protocol.protocol_import import ProtImportMovies
from pyworkflow.em.packages.xmipp3 import ProtMovieAlignment
from pyworkflow.em.packages.grigoriefflab import ProtCTFFind

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

ACCEPTED_EXTENSIONS = [
    'mrcs',
    'mrc',
    'dm4',
    'dm3',
]
PROJECT = ''
VOLTAGE = 200
SAMPLING_RATE = 1
PROCESSES = 1


def run_scipion_qc(movie_file):

    manager = Manager()
    project = manager.loadProject(PROJECT)
    path, pattern = os.path.split(movie_file)

    add_movies = project.newProtocol(
        ProtImportMovies,
        filesPath=path,
        filesPattern=pattern,
        voltage=VOLTAGE,
        samplingRate=SAMPLING_RATE,
    )
    project.launchProtocol(add_movies, wait=True)

    align_movies = project.newProtocol(
        ProtMovieAlignment,
        inputMovies=add_movies.outputMovies,
    )
    project.launchProtocol(align_movies, wait=True)

    find_ctf = project.newProtocol(
        ProtCTFFind,
        inputMicrographs=align_movies.outputMicrographs,
    )
    project.launchProtocol(find_ctf, wait=False)


class MyEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        size = -1

        # Loop to wait for file completion
        while os.path.getsize(event.src_path) != size:
            size = os.path.getsize(event.src_path)
            time.sleep(1)

        POOL.apply_async(run_scipion_qc, (event.src_path,))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--voltage', type=str, default='200',
                        help='Voltage used in acquisition')
    parser.add_argument('--sampling_rate', type=str, default='1',
                        help='Sampling rate used in acquisition')
    parser.add_argument('--processes', type=int, default=1,
                        help='Scipion processes to run in parallel')

    parser.add_argument('project', type=str, help='Scipion project')
    parser.add_argument('directory', type=str, help='Directory to monitor')
    args = parser.parse_args()

    PROJECT = args.project
    VOLTAGE = args.voltage
    SAMPLING_RATE = args.sampling_rate
    PROCESSES = args.processes

    POOL = Pool(processes=PROCESSES)

    # Create or load project
    manager = Manager()
    if manager.hasProject(PROJECT):
        project = manager.loadProject(PROJECT)
    else:
        project = manager.createProject(PROJECT)

    # First, start up watchdog
    event_handler = MyEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path=args.directory, recursive=False)
    observer.start()
    print('Watchdog started; to exit, press Control-C')

    # Second, run all existing files
    for f in os.listdir(args.directory):
        try:
            ext = os.path.splitext(f)[1].split('.')[1]
            if ext in ACCEPTED_EXTENSIONS:
                full_path = os.path.join(
                    args.directory, os.listdir(args.directory)[0]
                )
                POOL.apply_async(run_scipion_qc, (full_path,))
                time.sleep(0.1)  # Give time buffer to prevent orphan protocols
        except IndexError:  # Skips files with no extensions
            pass

    # Loop until KeyboardInterrupt
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
