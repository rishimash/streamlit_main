import glob
import pandas as pd

OS_PATH = '../OpenStore/'
SAVE_PATH = '../Archivetables/'


class FileReader:

    def __init__(self, SAVE_PATH = SAVE_PATH):
        self.SAVE_PATH = SAVE_PATH
        self.posts = self.read_files('posts')
        self.reports = self.read_files('reports')


    def read_files(self, report_type='posts'):
        rprts = {}
        paths = glob.glob(self.SAVE_PATH +'/' + report_type +'/*')
        for path in paths:
            rprts[path.split('/')[-1].split('.csv')[0]] = pd.read_csv(path, index_col=0).reset_index(drop=True)
        return rprts