import argparse
import h5py
import numpy as np
import os
import pandas as pd
from tqdm import tqdm

code_hdf5_filenames = [
    "exams_part0.hdf5",
    "exams_part1.hdf5",
    "exams_part2.hdf5",
    "exams_part3.hdf5",
    "exams_part4.hdf5",
    "exams_part5.hdf5",
    "exams_part6.hdf5",
    "exams_part7.hdf5",
    "exams_part8.hdf5",
    "exams_part9.hdf5",
    "exams_part10.hdf5",
    "exams_part11.hdf5",
    "exams_part12.hdf5",
    "exams_part13.hdf5",
    "exams_part14.hdf5",
    "exams_part15.hdf5",
    "exams_part16.hdf5",
    "exams_part17.hdf5",
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create virtual dataset')
    parser.add_argument('path', type=str, nargs='?', default=os.getcwd(), help='Path to the directory containing the unzipped files (default: current working directory)')
    parser.add_argument('destination', type=str, nargs='?', default=os.getcwd(), help='Directory for the .h5 files (default: current working directory)')

    args = parser.parse_args()

    # add args to filenames
    code_hdf5_filenames = [os.path.join(args.path, filename) for filename in code_hdf5_filenames]

    files = [h5py.File(ff, 'r') for ff in code_hdf5_filenames]

    ids = np.concatenate([ff['exam_id'][:-1] for ff in files])
    #regs = np.concatenate([ff['register_num'] for ff in files])

    f = h5py.File(os.path.join(args.destination, 'virtual.h5'), 'w')

    f.create_dataset('exam_id', data=ids, dtype='i8')
    #f.create_dataset('register_num', data=ids, dtype='i4')

    layout = h5py.VirtualLayout(shape=(len(ids), 4096, 12), dtype='f4')

    end = 0
    for i, file in enumerate(code_hdf5_filenames):
        start = end
        end = start + len(files[i]['exam_id'][:-1])
        vsource = h5py.VirtualSource(file, 'tracings', shape=(end-start, 4096, 12))
        layout[start:end, :, :] = vsource

    f.create_virtual_dataset('tracings', layout, fillvalue=0)
    f.close()

    # Load the data
    path_to_csv = os.path.join(args.path, 'exams.csv')
    path_to_h5 = os.path.join(args.destination, 'virtual.h5')

    df = pd.read_csv(path_to_csv, index_col='exam_id')

    # Get h5 file
    h5_file = h5py.File(path_to_h5, 'r')
    traces_ids = (h5_file['exam_id'])

    # Only keep the traces in the csv that match the traces in the h5 file
    df = df[df.index.isin(traces_ids)]

    # Define traces
    traces = h5_file['tracings']

    # Sort the dataframe in trace order
    df = df.reindex(traces_ids)

    # Divide the data into train and test set, without overlapping patient ids
    patient_ids = df['patient_id'].unique()

    np.random.seed(42)
    np.random.shuffle(patient_ids)

    train_size = int(0.8 * len(patient_ids))
    val_size = int(0.1 * len(patient_ids))
    test_size = len(patient_ids) - train_size - val_size
    train_patient_ids = patient_ids[:train_size]
    val_patient_ids = patient_ids[train_size:train_size + val_size]
    test_patient_ids = patient_ids[train_size + val_size:]

    train_df = df[df['patient_id'].isin(train_patient_ids)]
    val_df = df[df['patient_id'].isin(val_patient_ids)]
    test_df = df[df['patient_id'].isin(test_patient_ids)]

    # Len of train, val, and test
    no_train = len(train_df)
    no_val = len(val_df)
    no_test = len(test_df)

    print(f'Size of train: {len(train_df)}')
    print(f'Size of val: {len(val_df)}')
    print(f'Size of test: {len(test_df)}') 

    train_indices_to_keep = np.isin(df.index, train_df.index)
    val_indices_to_keep = np.isin(df.index, val_df.index)
    test_indices_to_keep = np.isin(df.index, test_df.index)

    # Split the h5 file into train, val, and test h5 files
    train_traces_indeces = h5_file['exam_id'][train_indices_to_keep]
    val_traces_indeces = h5_file['exam_id'][val_indices_to_keep]
    test_traces_indeces = h5_file['exam_id'][test_indices_to_keep]

    train_h5_file = h5py.File(os.path.join(args.destination,'train.h5'), 'w')
    val_h5_file = h5py.File(os.path.join(args.destination,'val.h5'), 'w')
    test_h5_file = h5py.File(os.path.join(args.destination,'test.h5'), 'w')

    train_h5_file.create_dataset('exam_id', data=train_traces_indeces, dtype='i8')
    val_h5_file.create_dataset('exam_id', data=val_traces_indeces, dtype='i8')
    test_h5_file.create_dataset('exam_id', data=test_traces_indeces, dtype='i8')

    train_to_save = None
    val_to_save = None
    test_to_save = None

    train_ind = 0
    val_ind = 0
    test_ind = 0

    for i, trace in tqdm(enumerate(traces), total=len(traces)):
        if train_indices_to_keep[i]:
            if train_to_save is None:
                train_to_save = train_h5_file.create_dataset('tracings', (no_train,) + traces[0].shape, dtype='f8')
            train_to_save[train_ind] = trace
            train_ind += 1
            
        if val_indices_to_keep[i]:
            if val_to_save is None:
                val_to_save = val_h5_file.create_dataset('tracings', (no_val,) + traces[0].shape, dtype='f8')
            val_to_save[val_ind] = trace
            val_ind += 1
            
        if test_indices_to_keep[i]:
            if test_to_save is None:
                test_to_save = test_h5_file.create_dataset('tracings', (no_test,) + traces[0].shape, dtype='f8')
            test_to_save[test_ind] = trace
            test_ind += 1

    train_h5_file.close()
    val_h5_file.close()
    test_h5_file.close()