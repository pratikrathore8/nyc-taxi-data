import psycopg2
import os, time
import subprocess
import pandas as pd
import h5py
import time
import numpy as np
import argparse

def make_copy_sql(from_id, num_rows, out_file):
    query = """
    COPY (
        SELECT
            id,
            EXTRACT(EPOCH FROM CAST(pickup_datetime AS time)) as time,
            EXTRACT(ISODOW FROM pickup_datetime) as dow,
            EXTRACT(DAY FROM pickup_datetime) as dom,
            EXTRACT(MONTH FROM pickup_datetime) as month,
            round(pickup_latitude, 6) as pickup_lat,
            round(pickup_longitude, 6) as pickup_lon,
            round(dropoff_latitude, 6) as dropoff_lat,
            round(dropoff_longitude, 6) as dropoff_lon,
            round(trip_distance, 3) as distance,
            EXTRACT(EPOCH FROM dropoff_datetime - pickup_datetime) as duration
        FROM trips 
        WHERE 
            (
                (EXTRACT(YEAR FROM pickup_datetime) < 2011 AND (
                    pickup_nyct2010_gid IS NOT NULL AND 
                    dropoff_nyct2010_gid IS NOT NULL
                )) OR
                (EXTRACT(YEAR FROM pickup_datetime) >= 2011 AND (
                    pickup_location_id < 264 AND 
                    dropoff_location_id < 264
                ))
            ) AND
            (EXTRACT(EPOCH FROM dropoff_datetime - pickup_datetime) BETWEEN 0 AND 18000) AND
            (id > %d)
        ORDER BY id ASC
        LIMIT %d
    ) TO '%s'
    WITH (FORMAT csv, HEADER true);
    """ % (from_id, num_rows, out_file)
    return query

def list_files(folder):
    for r, _, f in os.walk(folder):
        for file in f:
            if file.endswith('.csv.gz'):
                yield os.path.join(r, file)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", type=str)
    parser.add_argument("--save_dir", type=str)
    args = parser.parse_args()

    csv_size = 3_000_000
    save_folder = os.path.abspath(args.save_dir)
    try:
        os.makedirs(save_folder)
    except FileExistsError:
        pass

    conn = psycopg2.connect(database="nyc-taxi-data")

    index = 0
    i = 0
    with conn.cursor() as cursor:
        while True:
            t_s = time.time()
            fn = os.path.join(save_folder, "%d.csv" % (index))
            # Postgres server needs to have permission to create the file
            os.umask(0)
            with open(os.open(fn, os.O_CREAT | os.O_WRONLY, 0o777), 'w') as fh:
                cursor.copy_expert(make_copy_sql(index, csv_size, fn), fh)
            # Read last line of written file to check the new index
            last_line = os.popen('tail -n 1 %s' % (fn)).read()

            fh.close()

            try:
                index = int(last_line.split(",")[0])
            except ValueError:
                print("Error: %s" % (last_line))
                break
            # Compress the file
            subprocess.run(['gzip', '-f', fn], check=True)

            i += 1
            print("%d - %.2fs - Retrieved new start ID %d" % (i, time.time() - t_s, index))

    all_x = []
    all_y = []
    for f in list_files(save_folder):
        t_s = time.time()
        df = pd.read_csv(f, compression='gzip', header=0, index_col=False)
        Y = df['duration'].to_numpy(np.int32, copy=True)
        X = df[['time', 'dow', 'dom', 'month', 'pickup_lat',
                'pickup_lon', 'dropoff_lat', 'dropoff_lon',
                'distance']].to_numpy(np.float64, copy=True)
        all_x.append(X)
        all_y.append(Y)
        del df
        print("file %s read in %.2fs" % (f, time.time() - t_s))

    num_samples = sum([arr.shape[0] for arr in all_x])
    dim = all_x[0].shape[1]
    print(num_samples, ",", dim)

    max_chunk_size = 2 * 2**20  # 2MB
    chunk_x = int(max_chunk_size / dim / 8)
    chunk_y = chunk_x
    print("Chunk size:", chunk_x)

    h5py_name = args.filename + '.h5py'

    with h5py.File(os.path.join(save_folder, h5py_name), 'w', libver='latest') as f:
        Xdset = f.create_dataset("X", (num_samples, dim), dtype='float64', 
                                compression="gzip", chunks=(chunk_x, dim))
        Ydset = f.create_dataset("Y", (num_samples, 1), dtype='int32')
        current_i = 0
        for X, Y in zip(all_x, all_y):
            t_s = time.time()
            X = np.ascontiguousarray(X)
            Y = Y.reshape((-1, 1))
            Xdset.write_direct(X, dest_sel=np.s_[current_i:current_i+X.shape[0], :])
            Ydset.write_direct(Y, dest_sel=np.s_[current_i:current_i+Y.shape[0], :])
            current_i += X.shape[0]
            print("i: %d/%d in %.2fs" % (current_i, num_samples, time.time() - t_s))

if __name__ == '__main__':
    main()