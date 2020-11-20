import os
import csv


def split(filehandler, row_limit,
          output_name_template, output_path, delimiter=',', keep_headers=True):

    reader = csv.reader(open(filehandler,"r"),delimiter=',')
    current_piece = 1
    current_out_path = os.path.join(
        output_path,
        output_name_template % current_piece
    )
    current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
    current_limit = row_limit
    if keep_headers:
        headers = next(reader)
        current_out_writer.writerow(headers)
    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(
                output_path,
                output_name_template % current_piece
            )
            current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)


if __name__ == "__main__":
    dir = '../../raw_data/Med/NHDmed_flowlines_all-2/info/'
    output_dir = '../../raw_data/Med/NHDmed_flowlines_all-2/splited_infochunks/'
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    for filename in os.listdir(dir):
        filename_path = dir + filename
        save_name = os.path.splitext(filename)[0]
        split(filename_path, row_limit=5000,
              output_name_template=save_name + '_chunk%s.csv', output_path=output_dir)
