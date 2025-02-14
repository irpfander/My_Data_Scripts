import requests
import os
import zipfile


# Function to download file from url
def direct_download(url, file_path, datasets_to_pull):
    '''
    Function to download file from URL. Iterates through data writing out chunks of 1 MB (1024*1024)
    :param url: URL for the data to be downloaded
    :param file_path: File path where the data will be saved
    :param datasets_to_pull: name of dataset name to be pulled
    :return: None
    '''

    out_path = os.path.join(file_path, datasets_to_pull[0])
    r = requests.get(url, stream=True)
    with open(out_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024*1024):
            f.write(chunk)

    # unzip
    if out_path[-3:] == 'zip':
        with zipfile.ZipFile(out_path, 'r') as file:
            file.extractall(path=file_path)



