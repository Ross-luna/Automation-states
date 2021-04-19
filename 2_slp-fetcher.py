import pickle
from requests import get
import os
import argparse
from multiprocessing import Pool
import tqdm
import glob


# gets arguments from bash command 'python fetcher.py <pickle> <workfolder>'
parser = argparse.ArgumentParser()
parser.add_argument("list", help="pickle file to read from")
parser.add_argument("workfolder", help="workfolder to save files")
args = parser.parse_args()

filename = args.list
workfolder = args.workfolder

# generates a list of files already downloaded to skip them
try:
    inputs = pickle.load(open(filename , "rb"))
    print("success loading pickle: {}".format(filename))
except EOFError:
    print("u sure bro?")

left = glob.glob(workfolder + '/*/*.*')


docs_local = []
for l in left:
    e = l.split('/')[-1].split('.')[0]
    docs_local.append(e)


# iterates elements of the list of dictionaries, checks if the file
# already exists and if not, it downloads and save the file to the
# corresponding directory
def job(x):
    # do some work
    for f in x['files']:
        if "{}".format(str(f['driver']) + '_' + str(f['document'].split('_')[0])) in docs_local:
            print("skipping file because it already exists.")
            continue
        else:
            response = get(x['Doc Link'])
            if response.status_code == 200:
                for f in x['files']:
                    if 'circulacion' in str(f['document']):
                        fn = "{}/{}/".format(workfolder, 'vehiculo_' + str(f['plate']))
                        try:
                            os.mkdir(fn)
                            #print('Created directory for vehicle')
                        except FileExistsError:
                            pass

                        with open("{}{}.{}".format(fn, str(f['plate']) + '_' + str(f['document']), f['format']), "wb") as file:
                            #print('Creating new file')
                            file.write(response.content)
                    elif '_driver' in str(f['document']):
                        fn = "{}/{}/".format(workfolder, 'vehiculo_' + str(f['plate']))
                        try:
                            os.mkdir(fn)
                            #print('Created directory for partner, proceding to driver')
                        except FileExistsError:
                            pass

                        with open("{}{}.{}".format(fn, str(f['driver']) + '_' + str(f['document'])[0:len(f['document'])-len('_driver')], f['format']), "wb") as file:
                            #print('Creating new file')
                            file.write(response.content)
                    else:
                        continue
                        #print('Nothing to do here')

            else:
                print("server replied with an error. {}".format(response.status_code))

def main():

     with Pool() as pool:


        for _ in tqdm.tqdm(pool.imap_unordered(job, inputs), total=len(inputs)):
            pass

if __name__=="__main__":
    main()
